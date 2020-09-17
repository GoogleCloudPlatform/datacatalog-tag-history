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

import static com.google.cloud.solutions.catalogtagrecording.ProtoJsonConverter.asJsonString;
import static com.google.cloud.solutions.catalogtagrecording.ProtoJsonConverter.parseJson;
import static com.google.cloud.solutions.catalogtagrecording.testing.TestResourceLoader.load;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.EntityTagOperationRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.skyscreamer.jsonassert.JSONAssert;

@RunWith(JUnit4.class)
public final class ProtoToTableRowMapperTest {

  @Test
  public void apply_snakeCase_valid() {
    EntityTagOperationRecord record =
        parseJson(load("create_tag_operation_record.json"), EntityTagOperationRecord.class);

    TableRow row = ProtoToTableRowMapper.withSnakeCase().apply(record);

    JSONAssert.assertEquals(
        load("tablerow_snakecase_create_record.json"), asJsonString(row), /*strict=*/ false);
  }

  @Test
  public void apply_camelCase_valid() {
    EntityTagOperationRecord record =
        parseJson(load("create_tag_operation_record.json"), EntityTagOperationRecord.class);

    TableRow row = ProtoToTableRowMapper.withCamelCase().apply(record);

    JSONAssert.assertEquals(
        load("tablerow_camelcase_create_record.json"), asJsonString(row), /*strict=*/ false);
  }
}
