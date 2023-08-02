/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <cudf/copying.hpp>
#include <cudf/detail/copy.hpp>
#include <cudf/detail/iterator.cuh>
#include <cudf/detail/merge.cuh>
#include <cudf/detail/nvtx/ranges.hpp>
#include <cudf/detail/utilities/cuda.cuh>
#include <cudf/detail/utilities/vector_factories.hpp>
#include <cudf/dictionary/detail/merge.hpp>
#include <cudf/dictionary/detail/update_keys.hpp>
#include <cudf/strings/detail/merge.cuh>
#include <cudf/structs/structs_column_view.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_device_view.cuh>
#include <cudf/utilities/default_stream.hpp>
#include <cudf/utilities/traits.hpp>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_uvector.hpp>
#include <rmm/exec_policy.hpp>

#include <thrust/iterator/constant_iterator.h>
#include <thrust/iterator/counting_iterator.h>
#include <thrust/merge.h>
#include <thrust/pair.h>
#include <thrust/transform.h>
#include <thrust/tuple.h>

#include <queue>
#include <vector>

#include <cudf_test/column_utilities.hpp>

namespace cudf {
namespace detail {
namespace {

using detail::side;
using index_type = detail::index_type;

/**
 * @brief Merges the bits of two validity bitmasks.
 *
 * Merges the bits from two column_device_views into the destination validity buffer
 * according to `merged_indices` map such that bit `i` in `out_validity`
 * will be equal to bit `thrust::get<1>(merged_indices[i])` from `left_dcol`
 * if `thrust::get<0>(merged_indices[i])` equals `side::LEFT`; otherwise,
 * from `right_dcol`.
 *
 * `left_dcol` and `right_dcol` must not overlap.
 *
 * @tparam left_have_valids Indicates whether left_dcol mask is unallocated (hence, ALL_VALID)
 * @tparam right_have_valids Indicates whether right_dcol mask is unallocated (hence ALL_VALID)
 * @param[in] left_dcol The left column_device_view whose bits will be merged
 * @param[in] right_dcol The right column_device_view whose bits will be merged
 * @param[out] out_validity The output validity buffer after merging the left and right buffers
 * @param[in] num_destination_rows The number of rows in the out_validity buffer
 * @param[in] merged_indices The map that indicates the source of the input and index
 * to be copied to the output. Length must be equal to `num_destination_rows`
 */
template <bool left_have_valids, bool right_have_valids>
__global__ void materialize_merged_bitmask_kernel(
  column_device_view left_dcol,
  column_device_view right_dcol,
  bitmask_type* out_validity,
  size_type const num_destination_rows,
  index_type const* const __restrict__ merged_indices)
{
  size_type destination_row = threadIdx.x + blockIdx.x * blockDim.x;

  auto active_threads = __ballot_sync(0xffff'ffffu, destination_row < num_destination_rows);

  while (destination_row < num_destination_rows) {
    auto const [src_side, src_row] = merged_indices[destination_row];
    bool const from_left{src_side == side::LEFT};
    bool source_bit_is_valid{true};
    if (left_have_valids && from_left) {
      source_bit_is_valid = left_dcol.is_valid_nocheck(src_row);
    } else if (right_have_valids && !from_left) {
      source_bit_is_valid = right_dcol.is_valid_nocheck(src_row);
    }

    // Use ballot to find all valid bits in this warp and create the output
    // bitmask element
    bitmask_type const result_mask{__ballot_sync(active_threads, source_bit_is_valid)};

    // Only one thread writes output
    if (0 == threadIdx.x % warpSize) { out_validity[word_index(destination_row)] = result_mask; }

    destination_row += blockDim.x * gridDim.x;
    active_threads = __ballot_sync(active_threads, destination_row < num_destination_rows);
  }
}

void materialize_bitmask(column_view const& left_col,
                         column_view const& right_col,
                         bitmask_type* out_validity,
                         size_type num_elements,
                         index_type const* merged_indices,
                         rmm::cuda_stream_view stream)
{
  constexpr size_type BLOCK_SIZE{256};
  detail::grid_1d grid_config{num_elements, BLOCK_SIZE};

  auto p_left_dcol  = column_device_view::create(left_col, stream);
  auto p_right_dcol = column_device_view::create(right_col, stream);

  auto left_valid  = *p_left_dcol;
  auto right_valid = *p_right_dcol;

  if (left_col.has_nulls()) {
    if (right_col.has_nulls()) {
      materialize_merged_bitmask_kernel<true, true>
        <<<grid_config.num_blocks, grid_config.num_threads_per_block, 0, stream.value()>>>(
          left_valid, right_valid, out_validity, num_elements, merged_indices);
    } else {
      materialize_merged_bitmask_kernel<true, false>
        <<<grid_config.num_blocks, grid_config.num_threads_per_block, 0, stream.value()>>>(
          left_valid, right_valid, out_validity, num_elements, merged_indices);
    }
  } else {
    if (right_col.has_nulls()) {
      materialize_merged_bitmask_kernel<false, true>
        <<<grid_config.num_blocks, grid_config.num_threads_per_block, 0, stream.value()>>>(
          left_valid, right_valid, out_validity, num_elements, merged_indices);
    } else {
      CUDF_FAIL("materialize_merged_bitmask_kernel<false, false>() should never be called.");
    }
  }

  CUDF_CHECK_CUDA(stream.value());
}

struct side_index_generator {
  side _side;

  __device__ index_type operator()(size_type i) const noexcept { return index_type{_side, i}; }
};

/**
 * @brief Generates the row indices and source side (left or right) in accordance with the index
 * columns.
 *
 *
 * @tparam index_type Indicates the type to be used to collect index and side information;
 * @param[in] left_table The left table_view to be merged
 * @param[in] right_table The right table_view to be merged
 * @param[in] column_order Sort order types of index columns
 * @param[in] null_precedence Array indicating the order of nulls with respect to non-nulls for the
 * index columns
 * @param[in] stream CUDA stream used for device memory operations and kernel launches.
 *
 * @return A device_uvector of merged indices
 */
index_vector generate_merged_indices(table_view const& left_table,
                                     table_view const& right_table,
                                     std::vector<order> const& column_order,
                                     std::vector<null_order> const& null_precedence,
                                     rmm::cuda_stream_view stream)
{
  size_type const left_size  = left_table.num_rows();
  size_type const right_size = right_table.num_rows();
  size_type const total_size = left_size + right_size;

  auto left_gen    = side_index_generator{side::LEFT};
  auto right_gen   = side_index_generator{side::RIGHT};
  auto left_begin  = cudf::detail::make_counting_transform_iterator(0, left_gen);
  auto right_begin = cudf::detail::make_counting_transform_iterator(0, right_gen);

  index_vector merged_indices(total_size, stream);

  // auto lhs_device_view = table_device_view::create(left_table, stream);
  // auto rhs_device_view = table_device_view::create(right_table, stream);

  // auto d_column_order = cudf::detail::make_device_uvector_async(
  //   column_order, stream, rmm::mr::get_current_device_resource());
  auto left_comp = cudf::experimental::row::lexicographic::self_comparator{
    left_table, column_order, null_precedence, stream};
  auto left_right_comp = cudf::experimental::row::lexicographic::two_table_comparator{
    left_table, right_table, column_order, null_precedence, stream};
  auto right_comp = cudf::experimental::row::lexicographic::self_comparator{
    right_table, column_order, null_precedence, stream};

  auto const left_has_nulls       = nullate::DYNAMIC{cudf::has_nested_nulls(left_table)};
  auto const right_has_nulls      = nullate::DYNAMIC{cudf::has_nested_nulls(right_table)};
  auto const left_right_has_nulls = nullate::DYNAMIC{left_has_nulls or right_has_nulls};

  if (cudf::detail::has_nested_columns(left_table) or
      cudf::detail::has_nested_columns(right_table)) {
    auto d_left_comp       = left_comp.less<true>(left_has_nulls);
    auto d_left_right_comp = left_right_comp.less<true>(left_right_has_nulls);
    auto d_right_comp      = right_comp.less<true>(right_has_nulls);
    // auto d_null_precedence = cudf::detail::make_device_uvector_async(
    //   null_precedence, stream, rmm::mr::get_current_device_resource());

    auto ineq_op =
      detail::row_lexicographic_tagged_comparator(d_left_comp, d_left_right_comp, d_right_comp);
    thrust::merge(rmm::exec_policy(stream),
                  left_begin,
                  left_begin + left_size,
                  right_begin,
                  right_begin + right_size,
                  merged_indices.begin(),
                  ineq_op);
  } else {
    auto d_left_comp       = left_comp.less<false>(left_has_nulls);
    auto d_left_right_comp = left_right_comp.less<false>(left_right_has_nulls);
    auto d_right_comp      = right_comp.less<false>(right_has_nulls);

    auto ineq_op =
      detail::row_lexicographic_tagged_comparator(d_left_comp, d_left_right_comp, d_right_comp);
    thrust::merge(rmm::exec_policy(stream),
                  left_begin,
                  left_begin + left_size,
                  right_begin,
                  right_begin + right_size,
                  merged_indices.begin(),
                  ineq_op);
  }

  CUDF_CHECK_CUDA(stream.value());

  return merged_indices;
}

/**
 * @brief Generate merged column given row-order of merged tables
 *  (ordered according to indices of key_cols) and the 2 columns to merge.
 */
struct column_merger {
  explicit column_merger(index_vector const& row_order) : row_order_(row_order) {}

  template <typename Element, CUDF_ENABLE_IF(not is_rep_layout_compatible<Element>())>
  std::unique_ptr<column> operator()(column_view const&,
                                     column_view const&,
                                     rmm::cuda_stream_view,
                                     rmm::mr::device_memory_resource*) const
  {
    CUDF_FAIL("Unsupported type for merge.");
  }

  // column merger operator;
  //
  template <typename Element>
  std::enable_if_t<is_rep_layout_compatible<Element>(), std::unique_ptr<column>> operator()(
    column_view const& lcol,
    column_view const& rcol,
    rmm::cuda_stream_view stream,
    rmm::mr::device_memory_resource* mr) const
  {
    auto lsz         = lcol.size();
    auto merged_size = lsz + rcol.size();
    auto merged_col  = cudf::detail::allocate_like(lcol.has_nulls() ? lcol : rcol,
                                                  merged_size,
                                                  cudf::mask_allocation_policy::RETAIN,
                                                  stream,
                                                  mr);

    //"gather" data from lcol, rcol according to row_order_ "map"
    //(directly calling gather() won't work because
    // lcol, rcol indices overlap!)
    //
    cudf::mutable_column_view merged_view = merged_col->mutable_view();

    // initialize null_mask to all valid:
    //
    // Note: this initialization in conjunction with
    // _conditionally_ calling materialize_bitmask() below covers
    // the case materialize_merged_bitmask_kernel<false, false>()
    // which won't be called anymore (because of the _condition_
    // below)
    //
    cudf::detail::set_null_mask(merged_view.null_mask(), 0, merged_view.size(), true, stream);

    // set the null count:
    //
    merged_col->set_null_count(lcol.null_count() + rcol.null_count());

    // to resolve view.data()'s types use: Element
    //
    auto const d_lcol = lcol.data<Element>();
    auto const d_rcol = rcol.data<Element>();

    // capture lcol, rcol
    // and "gather" into merged_view.data()[indx_merged]
    // from lcol or rcol, depending on side;
    //
    thrust::transform(rmm::exec_policy(stream),
                      row_order_.begin(),
                      row_order_.end(),
                      merged_view.begin<Element>(),
                      [d_lcol, d_rcol] __device__(index_type const& index_pair) {
                        auto const [side, index] = index_pair;
                        return side == side::LEFT ? d_lcol[index] : d_rcol[index];
                      });

    // CAVEAT: conditional call below is erroneous without
    // set_null_mask() call (see TODO above):
    //
    if (lcol.has_nulls() || rcol.has_nulls()) {
      // resolve null mask:
      //
      materialize_bitmask(
        lcol, rcol, merged_view.null_mask(), merged_view.size(), row_order_.data(), stream);
    }

    return merged_col;
  }

 private:
  index_vector const& row_order_;
};

// specialization for strings
template <>
std::unique_ptr<column> column_merger::operator()<cudf::string_view>(
  column_view const& lcol,
  column_view const& rcol,
  rmm::cuda_stream_view stream,
  rmm::mr::device_memory_resource* mr) const
{
  auto column = strings::detail::merge<index_type>(strings_column_view(lcol),
                                                   strings_column_view(rcol),
                                                   row_order_.begin(),
                                                   row_order_.end(),
                                                   stream,
                                                   mr);
  if (lcol.has_nulls() || rcol.has_nulls()) {
    auto merged_view = column->mutable_view();
    materialize_bitmask(
      lcol, rcol, merged_view.null_mask(), merged_view.size(), row_order_.data(), stream);
  }
  return column;
}

// specialization for dictionary
template <>
std::unique_ptr<column> column_merger::operator()<cudf::dictionary32>(
  column_view const& lcol,
  column_view const& rcol,
  rmm::cuda_stream_view stream,
  rmm::mr::device_memory_resource* mr) const
{
  auto result = cudf::dictionary::detail::merge(
    cudf::dictionary_column_view(lcol), cudf::dictionary_column_view(rcol), row_order_, stream, mr);

  // set the validity mask
  if (lcol.has_nulls() || rcol.has_nulls()) {
    auto merged_view = result->mutable_view();
    materialize_bitmask(
      lcol, rcol, merged_view.null_mask(), merged_view.size(), row_order_.data(), stream);
  }
  return result;
}

// specialization for structs
template <>
std::unique_ptr<column> column_merger::operator()<cudf::struct_view>(
  column_view const& lcol,
  column_view const& rcol,
  rmm::cuda_stream_view stream,
  rmm::mr::device_memory_resource* mr) const
{
  // merge each child.
  auto const lhs = structs_column_view{lcol};
  auto const rhs = structs_column_view{rcol};

  auto it = cudf::detail::make_counting_transform_iterator(
    0, [&, merger = column_merger{row_order_}](size_type i) {
      return cudf::type_dispatcher<dispatch_storage_type>(lhs.child(i).type(),
                                                          merger,
                                                          lhs.get_sliced_child(i, stream),
                                                          rhs.get_sliced_child(i, stream),
                                                          stream,
                                                          mr);
    });

  auto merged_children   = std::vector<std::unique_ptr<column>>(it, it + lhs.num_children());
  auto const merged_size = lcol.size() + rcol.size();

  // materialize the output buffer
  rmm::device_buffer validity =
    lcol.has_nulls() || rcol.has_nulls()
      ? create_null_mask(merged_size, mask_state::UNINITIALIZED, stream, mr)
      : rmm::device_buffer{};
  if (lcol.has_nulls() || rcol.has_nulls()) {
    materialize_bitmask(lcol,
                        rcol,
                        static_cast<bitmask_type*>(validity.data()),
                        merged_size,
                        row_order_.data(),
                        stream);
  }

  return make_structs_column(merged_size,
                             std::move(merged_children),
                             lcol.null_count() + rcol.null_count(),
                             std::move(validity),
                             stream,
                             mr);
}

using table_ptr_type = std::unique_ptr<cudf::table>;

table_ptr_type merge(cudf::table_view const& left_table,
                     cudf::table_view const& right_table,
                     std::vector<cudf::size_type> const& key_cols,
                     std::vector<cudf::order> const& column_order,
                     std::vector<cudf::null_order> const& null_precedence,
                     rmm::cuda_stream_view stream,
                     rmm::mr::device_memory_resource* mr)
{
  // collect index columns for lhs, rhs, resp.
  //
  cudf::table_view index_left_view{left_table.select(key_cols)};
  cudf::table_view index_right_view{right_table.select(key_cols)};

  // extract merged row order according to indices:
  //
  auto const merged_indices = generate_merged_indices(
    index_left_view, index_right_view, column_order, null_precedence, stream);

  // create merged table:
  //
  auto const n_cols = left_table.num_columns();
  std::vector<std::unique_ptr<column>> merged_cols;
  merged_cols.reserve(n_cols);

  column_merger merger{merged_indices};
  transform(left_table.begin(),
            left_table.end(),
            right_table.begin(),
            std::back_inserter(merged_cols),
            [&](auto const& left_col, auto const& right_col) {
              return cudf::type_dispatcher<dispatch_storage_type>(
                left_col.type(), merger, left_col, right_col, stream, mr);
            });

  return std::make_unique<cudf::table>(std::move(merged_cols));
}

struct merge_queue_item {
  table_view view;
  table_ptr_type table;
  // Priority is a separate member to ensure that moving from an object
  // does not change its priority (which would ruin the queue invariant)
  cudf::size_type priority = 0;

  merge_queue_item(table_view const& view, table_ptr_type&& table)
    : view{view}, table{std::move(table)}, priority{-view.num_rows()}
  {
  }

  bool operator<(merge_queue_item const& other) const { return priority < other.priority; }
};

// Helper function to ensure that moving out of the priority_queue is "atomic"
template <typename T>
T top_and_pop(std::priority_queue<T>& q)
{
  auto moved = std::move(const_cast<T&>(q.top()));
  q.pop();
  return moved;
}

}  // anonymous namespace

table_ptr_type merge(std::vector<table_view> const& tables_to_merge,
                     std::vector<cudf::size_type> const& key_cols,
                     std::vector<cudf::order> const& column_order,
                     std::vector<cudf::null_order> const& null_precedence,
                     rmm::cuda_stream_view stream,
                     rmm::mr::device_memory_resource* mr)
{
  if (tables_to_merge.empty()) { return std::make_unique<cudf::table>(); }

  auto const& first_table = tables_to_merge.front();
  auto const n_cols       = first_table.num_columns();

  CUDF_EXPECTS(std::all_of(tables_to_merge.cbegin(),
                           tables_to_merge.cend(),
                           [n_cols](auto const& tbl) { return n_cols == tbl.num_columns(); }),
               "Mismatched number of columns");
  CUDF_EXPECTS(
    std::all_of(tables_to_merge.cbegin(),
                tables_to_merge.cend(),
                [&](auto const& tbl) { return cudf::have_same_types(first_table, tbl); }),
    "Mismatched column types");

  CUDF_EXPECTS(!key_cols.empty(), "Empty key_cols");
  CUDF_EXPECTS(key_cols.size() <= static_cast<size_t>(n_cols), "Too many values in key_cols");

  CUDF_EXPECTS(key_cols.size() == column_order.size(),
               "Mismatched size between key_cols and column_order");

  // This utility will ensure all corresponding dictionary columns have matching keys.
  // It will return any new dictionary columns created as well as updated table_views.
  auto matched = cudf::dictionary::detail::match_dictionaries(
    tables_to_merge, stream, rmm::mr::get_current_device_resource());
  auto merge_tables = matched.second;

  // A queue of (table view, table) pairs
  std::priority_queue<merge_queue_item> merge_queue;
  // The table pointer is null if we do not own the table (input tables)
  std::for_each(merge_tables.begin(), merge_tables.end(), [&](auto const& table) {
    if (table.num_rows() > 0) merge_queue.emplace(table, table_ptr_type());
  });

  // If there is only one non-empty table_view, return its copy
  if (merge_queue.size() == 1) {
    return std::make_unique<cudf::table>(merge_queue.top().view, stream, mr);
  }
  // No inputs have rows, return a table with same columns as the first one
  if (merge_queue.empty()) { return empty_like(first_table); }

  // Pick the two smallest tables and merge them
  // Until there is only one table left in the queue
  while (merge_queue.size() > 1) {
    // To delete the intermediate table at the end of the block
    auto const left_table = top_and_pop(merge_queue);
    // Deallocated at the end of the block
    auto const right_table = top_and_pop(merge_queue);

    // Only use mr for the output table
    auto const& new_tbl_mr = merge_queue.empty() ? mr : rmm::mr::get_current_device_resource();
    auto merged_table      = merge(left_table.view,
                              right_table.view,
                              key_cols,
                              column_order,
                              null_precedence,
                              stream,
                              new_tbl_mr);

    auto const merged_table_view = merged_table->view();
    merge_queue.emplace(merged_table_view, std::move(merged_table));
  }

  return std::move(top_and_pop(merge_queue).table);
}

}  // namespace detail

std::unique_ptr<cudf::table> merge(std::vector<table_view> const& tables_to_merge,
                                   std::vector<cudf::size_type> const& key_cols,
                                   std::vector<cudf::order> const& column_order,
                                   std::vector<cudf::null_order> const& null_precedence,
                                   rmm::mr::device_memory_resource* mr)
{
  CUDF_FUNC_RANGE();
  return detail::merge(
    tables_to_merge, key_cols, column_order, null_precedence, cudf::get_default_stream(), mr);
}

}  // namespace cudf
