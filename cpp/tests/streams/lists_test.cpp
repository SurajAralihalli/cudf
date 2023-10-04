/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

#include <cudf/lists/combine.hpp>

#include <cudf_test/base_fixture.hpp>
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/default_stream.hpp>

class ListTest : public cudf::test::BaseFixture {};

TEST_F(ListTest, ConcatenateRows)
{
  cudf::test::lists_column_wrapper<int> l1{{0, 1}, {2, 3}, {4, 5}};
  cudf::test::lists_column_wrapper<int> l2{{0, 1}, {2, 3}, {4, 5}};
  cudf::table_view lists_table({l1, l2});
  cudf::lists::concatenate_rows(
    lists_table, cudf::lists::concatenate_null_policy::IGNORE, cudf::test::get_default_stream());
}
