/*
 * Copyright 2020 The Data Catalog Tag History Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions.catalogtagrecording;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

public class JsonMessageParserTest {

  private static final String TEST_JSON =
      "{\n"
          + "  \"code\": 403,\n"
          + "  \"errors\": [\n"
          + "    {\n"
          + "      \"domain\": \"global\",\n"
          + "      \"message\": \"Access Denied\",\n"
          + "      \"reason\": \"accessDenied\"\n"
          + "    }\n"
          + "  ],\n"
          + "  \"status\": \"PERMISSION_DENIED\",\n"
          + "  \"keyWithNestedValues\": {\n"
          + "    \"nestedKey1\": \"nestedValue1\",\n"
          + "    \"secondLevelNestedKey\": {\n"
          + "      \"keyInSecond\": \"ValueInSecond\"\n"
          + "    }\n"
          + "  }\n"
          + "}";

  @Test
  public void getJson_completeJson() {
    JSONAssert.assertEquals(
        TEST_JSON, JsonMessageParser.of(TEST_JSON).getJson(), /*strict=*/ false);
  }

  @Test
  public void getRootPath_defaultRoot_equalsDollar() {
    assertThat(JsonMessageParser.of(TEST_JSON).getRootPath()).isEqualTo("$");
  }

  @Test
  public void getRootPath_nestedRoot_equalsCompleteRoot() {
    assertThat(
        JsonMessageParser.of(TEST_JSON)
            .forSubNode("$.keyWithNestedValues")
            .forSubNode("$.secondLevelNestedKey")
            .getRootPath())
        .isEqualTo("$.keyWithNestedValues.secondLevelNestedKey");
  }

  @Test
  public void read_emptyRootSingleStringValue_correct() {
    assertThat(JsonMessageParser.of(TEST_JSON).<String>read("$.status"))
        .isEqualTo("PERMISSION_DENIED");
  }

  @Test
  public void read_emptyRootSingleIntegerValue_correct() {
    assertThat(JsonMessageParser.of(TEST_JSON).<Integer>read("$.code")).isEqualTo(403);
  }

  @Test
  public void read_subNodeRootSingleValue_correct() {
    assertThat(
        JsonMessageParser.of(TEST_JSON)
            .forSubNode("$.keyWithNestedValues")
            .<String>read("$.nestedKey1"))
        .isEqualTo("nestedValue1");
  }

  @Test
  public void read_subNodeSecondLevelNestingRootSingleValue_correct() {
    assertThat(
        JsonMessageParser.of(TEST_JSON)
            .forSubNode("$.keyWithNestedValues")
            .forSubNode("$.secondLevelNestedKey")
            .<String>read("$.keyInSecond"))
        .isEqualTo("ValueInSecond");
  }

  @Test
  public void read_invalidKey_null() {
    assertThat(JsonMessageParser.of(TEST_JSON).<Object>read("$.missingKey")).isNull();
  }

  @Test
  public void readOrDefault_keyPresent_actualValue() {
    assertThat(JsonMessageParser.of(TEST_JSON).readOrDefault("$.status", "DEFAULT"))
        .isEqualTo("PERMISSION_DENIED");
  }

  @Test
  public void readOrDefault_keyMissing_defaultValue() {
    assertThat(JsonMessageParser.of(TEST_JSON).readOrDefault("$.status2", "DEFAULT"))
        .isEqualTo("DEFAULT");
  }

  @Test
  public void readOrDefault_nullKey_defaultValue() {
    assertThat(JsonMessageParser.of(TEST_JSON).readOrDefault(null, "DEFAULT")).isEqualTo("DEFAULT");
  }

  @Test
  public void setMessageJson_null_throwsNullPointerException() {
    assertThrows(IllegalArgumentException.class, () -> JsonMessageParser.of(null));
  }

  @Test
  public void setMessageJson_empty_throwsNullPointerException() {
    assertThrows(IllegalArgumentException.class, () -> JsonMessageParser.of(""));
  }

  @Test
  public void setMessageJson_blank_throwsNullPointerException() {
    assertThrows(IllegalArgumentException.class, () -> JsonMessageParser.of("  "));
  }
}
