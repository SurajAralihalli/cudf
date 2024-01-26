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
#include <cudf_test/table_utilities.hpp>

#include <cudf/groupby.hpp>
#include <cudf/io/parquet.hpp>

#include <nvbench/nvbench.cuh>

void bench_groupby_nvsum(nvbench::state& state)
{
  cudf::test::strings_column_wrapper col0({"", "", "", "", ""});
  cudf::test::fixed_width_column_wrapper<int32_t> col1{{1, 2, 3, 4, 5}};

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

NVBENCH_BENCH(bench_groupby_nvsum).set_name("groupby_nvsum");
