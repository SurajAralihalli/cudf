/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

#include <benchmarks/common/generate_input.hpp>

#include <cudf_test/column_utilities.hpp>
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/debug_utilities.hpp>
#include <cudf_test/iterator_utilities.hpp>
#include <cudf_test/table_utilities.hpp>

#include <cudf/groupby.hpp>
#include <cudf/io/parquet.hpp>

#include <nvbench/nvbench.cuh>

// detail::hash::groupby
void bench_groupby_nvsum1(nvbench::state& state)
{
  auto const path = state.get_string("path");

  auto read_opts   = cudf::io::parquet_reader_options_builder(cudf::io::source_info{path}).build();
  auto read_result = cudf::io::read_parquet(read_opts);
  auto t           = read_result.tbl->view();

  // cudf::test::print(t.column(0));
  // cudf::test::print(t.column(1));

  cudf::groupby::groupby grouper(
    cudf::table_view({t.column(0)}), cudf::null_policy::INCLUDE, cudf::sorted::NO);
  std::vector<cudf::groupby::aggregation_request> requests;
  requests.emplace_back(cudf::groupby::aggregation_request());
  requests[0].values = t.column(1);
  requests[0].aggregations.push_back(cudf::make_sum_aggregation<cudf::groupby_aggregation>());

  state.exec(nvbench::exec_tag::sync, [&](nvbench::launch& launch) {
    auto result = grouper.aggregate(requests, cudf::test::get_default_stream());
  });
}
template <typename Type>
void bench_groupby_nvsum2(nvbench::state& state, nvbench::type_list<Type>)
{
  std::vector<std::string> keys(2150983, "");
  std::vector<int> values(2150983, 5000);
  cudf::test::strings_column_wrapper col0(
    keys.begin(), keys.end(), cudf::test::iterators::all_nulls());

  cudf::test::fixed_width_column_wrapper<Type> col1(values.begin(), values.end());

  // cudf::test::print(col0);
  // cudf::test::print(col1);

  cudf::groupby::groupby grouper(
    cudf::table_view({col0}), cudf::null_policy::INCLUDE, cudf::sorted::NO);
  std::vector<cudf::groupby::aggregation_request> requests;
  requests.emplace_back(cudf::groupby::aggregation_request());
  requests[0].values = col1;
  requests[0].aggregations.push_back(cudf::make_sum_aggregation<cudf::groupby_aggregation>());

  state.exec(nvbench::exec_tag::sync, [&](nvbench::launch& launch) {
    auto result = grouper.aggregate(requests, cudf::test::get_default_stream());
  });
}

// sort_aggregate
void bench_groupby_nvsum3(nvbench::state& state)
{
  auto const path = state.get_string("path");

  auto read_opts   = cudf::io::parquet_reader_options_builder(cudf::io::source_info{path}).build();
  auto read_result = cudf::io::read_parquet(read_opts);
  auto t           = read_result.tbl->view();

  // cudf::test::print(t.column(0));
  // cudf::test::print(t.column(1));

  std::vector<cudf::groupby::aggregation_request> requests;
  requests.emplace_back(cudf::groupby::aggregation_request());
  requests[0].values = t.column(1);
  requests[0].aggregations.push_back(cudf::make_sum_aggregation<cudf::groupby_aggregation>());
  requests[0].aggregations.push_back(
    cudf::make_nth_element_aggregation<cudf::groupby_aggregation>(0));

  cudf::groupby::groupby grouper(
    cudf::table_view({t.column(0)}), cudf::null_policy::INCLUDE, cudf::sorted::NO);

  state.exec(nvbench::exec_tag::sync, [&](nvbench::launch& launch) {
    auto result = grouper.aggregate(requests, cudf::test::get_default_stream());
  });
}

NVBENCH_BENCH(bench_groupby_nvsum1)
  .set_name("groupby_nvsum1")
  .add_string_axis("path", {"/home/saralihalli/Downloads/testdata.parquet"});

// using data_type = nvbench::type_list<int32_t, uint32_t, int64_t, uint64_t>;

using data_type = nvbench::type_list<uint64_t>;

NVBENCH_BENCH_TYPES(bench_groupby_nvsum2, NVBENCH_TYPE_AXES(data_type)).set_name("groupby_nvsum2");

NVBENCH_BENCH(bench_groupby_nvsum3)
  .set_name("groupby_nvsum3")
  .add_string_axis("path", {"/home/saralihalli/Downloads/testdata.parquet"});
