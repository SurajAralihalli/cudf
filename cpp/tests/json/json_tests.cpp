/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

#include <cudf/json/json.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/strings/replace.hpp>
#include <cudf/strings/strings_column_view.hpp>

#include <cudf_test/base_fixture.hpp>
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/debug_utilities.hpp>
#include <cudf_test/testing_main.hpp>

#include <stdexcept>

// reference:  https://jsonpath.herokuapp.com/

// clang-format off
std::string json_string1{
  "{"
    "\'a\': \'A\'\'"
  "}"
};

std::string json_string2{
  "{"
    "\'a\': \'A\"\'"
  "}"
};

std::string json_string3{
  "{"
    "\'a\': \'\'A\'\'"
  "}"
};

std::string json_string4{
  "{"
    "\'a\': \'\"A\"\'"
  "}"
};

std::string json_string5{
  "{"
    "\'a\': \'\"A\'"
  "}"
};

std::string json_string6{
  "{"
    "\'a\': \'\'A\'"
  "}"
};

std::string json_string7{
  "{"
    "\'a\': \'\"A\'\'"
  "}"
};

std::string json_string8{
  "{"
    "\"a\": \"A\"\""
  "}"
};
// clang-format on

std::unique_ptr<cudf::column> drop_whitespace(cudf::column_view const& col)
{
  cudf::test::strings_column_wrapper whitespace{"\n", "\r", "\t"};
  cudf::test::strings_column_wrapper repl{"", "", ""};

  cudf::strings_column_view strings(col);
  cudf::strings_column_view targets(whitespace);
  cudf::strings_column_view replacements(repl);
  return cudf::strings::replace(strings, targets, replacements);
}

struct JsonPathTests : public cudf::test::BaseFixture {};

TEST_F(JsonPathTests, GetJsonObjectRootOp1)
{
  // root
  cudf::test::strings_column_wrapper input{json_string1};
  std::string json_path("$");
  auto options = cudf::get_json_object_options{};
  options.set_allow_single_quotes(true);
  auto result_raw = cudf::get_json_object(cudf::strings_column_view(input), json_path, options);
  auto result     = drop_whitespace(*result_raw);

  auto expected = drop_whitespace(input);

  cudf::test::print(*result);
  cudf::test::print(*expected);

  CUDF_TEST_EXPECT_COLUMNS_EQUIVALENT(*result, *expected);
}

TEST_F(JsonPathTests, GetJsonObjectRootOp2)
{
  // root
  cudf::test::strings_column_wrapper input{json_string2};
  std::string json_path("$");
  auto options = cudf::get_json_object_options{};
  options.set_allow_single_quotes(true);
  auto result_raw = cudf::get_json_object(cudf::strings_column_view(input), json_path, options);
  auto result     = drop_whitespace(*result_raw);

  auto expected = drop_whitespace(input);

  cudf::test::print(*result);
  cudf::test::print(*expected);

  CUDF_TEST_EXPECT_COLUMNS_EQUIVALENT(*result, *expected);
}

TEST_F(JsonPathTests, GetJsonObjectRootOp3)
{
  // root
  cudf::test::strings_column_wrapper input{json_string3};
  std::string json_path("$");
  auto options = cudf::get_json_object_options{};
  options.set_allow_single_quotes(true);
  auto result_raw = cudf::get_json_object(cudf::strings_column_view(input), json_path, options);
  auto result     = drop_whitespace(*result_raw);

  auto expected = drop_whitespace(input);

  cudf::test::print(*result);
  cudf::test::print(*expected);

  CUDF_TEST_EXPECT_COLUMNS_EQUIVALENT(*result, *expected);
}

TEST_F(JsonPathTests, GetJsonObjectRootOp4)
{
  // root
  cudf::test::strings_column_wrapper input{json_string4};
  std::string json_path("$");
  auto options = cudf::get_json_object_options{};
  options.set_allow_single_quotes(true);
  auto result_raw = cudf::get_json_object(cudf::strings_column_view(input), json_path, options);
  auto result     = drop_whitespace(*result_raw);

  auto expected = drop_whitespace(input);

  cudf::test::print(*result);
  cudf::test::print(*expected);

  CUDF_TEST_EXPECT_COLUMNS_EQUIVALENT(*result, *expected);
}

TEST_F(JsonPathTests, GetJsonObjectRootOp5)
{
  // root
  cudf::test::strings_column_wrapper input{json_string5};
  std::string json_path("$");
  auto options = cudf::get_json_object_options{};
  options.set_allow_single_quotes(true);
  auto result_raw = cudf::get_json_object(cudf::strings_column_view(input), json_path, options);
  auto result     = drop_whitespace(*result_raw);

  auto expected = drop_whitespace(input);

  cudf::test::print(*result);
  cudf::test::print(*expected);

  CUDF_TEST_EXPECT_COLUMNS_EQUIVALENT(*result, *expected);
}

TEST_F(JsonPathTests, GetJsonObjectRootOp6)
{
  // root
  cudf::test::strings_column_wrapper input{json_string6};
  std::string json_path("$");
  auto options = cudf::get_json_object_options{};
  options.set_allow_single_quotes(true);
  auto result_raw = cudf::get_json_object(cudf::strings_column_view(input), json_path, options);
  auto result     = drop_whitespace(*result_raw);

  auto expected = drop_whitespace(input);

  cudf::test::print(*result);
  cudf::test::print(*expected);

  CUDF_TEST_EXPECT_COLUMNS_EQUIVALENT(*result, *expected);
}

TEST_F(JsonPathTests, GetJsonObjectRootOp7)
{
  // root
  cudf::test::strings_column_wrapper input{json_string7};
  std::string json_path("$");
  auto options = cudf::get_json_object_options{};
  options.set_allow_single_quotes(true);
  auto result_raw = cudf::get_json_object(cudf::strings_column_view(input), json_path, options);
  auto result     = drop_whitespace(*result_raw);

  auto expected = drop_whitespace(input);

  cudf::test::print(*result);
  cudf::test::print(*expected);

  CUDF_TEST_EXPECT_COLUMNS_EQUIVALENT(*result, *expected);
}

TEST_F(JsonPathTests, GetJsonObjectRootOp8)
{
  // root
  cudf::test::strings_column_wrapper input{json_string8};
  std::string json_path("$");
  auto options = cudf::get_json_object_options{};
  options.set_allow_single_quotes(true);
  auto result_raw = cudf::get_json_object(cudf::strings_column_view(input), json_path, options);
  auto result     = drop_whitespace(*result_raw);

  auto expected = drop_whitespace(input);

  cudf::test::print(*result);
  cudf::test::print(*expected);

  CUDF_TEST_EXPECT_COLUMNS_EQUIVALENT(*result, *expected);
}

// Fail
// {'a': 'A''}
// {'a': '"A''}
// {"a": "A""}

// Pass
// {'a': 'A"'}
// {'a': ''A''}
// {'a': '"A"'}
// {'a': '"A'}
