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

import static com.google.cloud.solutions.catalogtagrecording.CatalogTagConverter.toCatalogTag;
import static com.google.cloud.solutions.catalogtagrecording.ProtoJsonConverter.parseJson;
import static com.google.cloud.solutions.catalogtagrecording.testing.TestResourceLoader.load;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.CatalogTag;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@SuppressWarnings("ConstantConditions") // Suppress Null checks for Proto-parser output.
@RunWith(JUnit4.class)
public final class CatalogTagConverterTest {

  @Test
  public void toCatalogTag_tagWithBooleanStringTimestampNoColumn_valid() {
    Tag tag = parseJson(load("single_tag_no_column.json"), Tag.class);
    assertThat(toCatalogTag(tag))
        .isEqualTo(
            parseJson(
                "{\n"
                    + "  \"tagId\": \"projects/my-project-id/locations/us/entryGroups/@bigquery/entries/cHJvamVjdHMvZ29vZ2xlLmNvbTphbmFudGQvZGF0YXNldHMvTXlEYXRhU2V0L3RhYmxlcy9Nb2NrUGlpUHJvY2Vzc2Vk/tags/CUzSbDlKy_z_bF\",\n"
                    + "  \"templateId\": \"projects/my-project-id/locations/us/tagTemplates/sample_catalog_tag\",\n"
                    + "  \"templateName\": \"Testing Sample Catalog Tag\",\n"
                    + "  \"fields\": [{\n"
                    + "    \"fieldId\": \"primary_source\",\n"
                    + "    \"fieldName\": \"Primary Source\",\n"
                    + "    \"kind\": \"BOOL\",\n"
                    + "    \"boolValue\": true\n"
                    + "  }, {\n"
                    + "    \"fieldId\": \"business_unit\",\n"
                    + "    \"fieldName\": \"Business Unit\",\n"
                    + "    \"kind\": \"STRING\",\n"
                    + "    \"stringValue\": \"Professional Services\"\n"
                    + "  }, {\n"
                    + "    \"fieldId\": \"updated_timestamp\",\n"
                    + "    \"fieldName\": \"Updated Timestamp\",\n"
                    + "    \"kind\": \"TIMESTAMP\",\n"
                    + "    \"timestampValue\": \"2020-09-09T15:25:00Z\"\n"
                    + "  }]\n"
                    + "}",
                CatalogTag.class));
  }

  @Test
  public void toCatalogTag_tagWithBooleanStringDoubleColumn_validColumn() {
    Tag tag = parseJson(load("single_tag_with_column.json"), Tag.class);

    assertThat(toCatalogTag(tag))
        .isEqualTo(
            parseJson(
                "{\n"
                    + "  \"tagId\": \"projects/my-project-id/locations/us/entryGroups/@bigquery/entries/cHJvamVjdHMvZ29vZ2xlLmNvbTphbmFudGQvZGF0YXNldHMvTXlEYXRhU2V0L3RhYmxlcy9Nb2NrUGlpUHJvY2Vzc2Vk/tags/CUzSbDlKy_z_bF\",\n"
                    + "  \"templateId\": \"projects/my-project-id/locations/us/tagTemplates/sample_catalog_tag\",\n"
                    + "  \"templateName\": \"Testing Sample Catalog Tag\",\n"
                    + "  \"column\": \"someColumn\",\n"
                    + "  \"fields\": [{\n"
                    + "    \"fieldId\": \"primary_source\",\n"
                    + "    \"fieldName\": \"Primary Source\",\n"
                    + "    \"kind\": \"BOOL\",\n"
                    + "    \"boolValue\": true\n"
                    + "  }, {\n"
                    + "    \"fieldId\": \"business_unit\",\n"
                    + "    \"fieldName\": \"Business Unit\",\n"
                    + "    \"kind\": \"STRING\",\n"
                    + "    \"stringValue\": \"Professional Services\"\n"
                    + "  }, {\n"
                    + "    \"fieldId\": \"version_number\",\n"
                    + "    \"fieldName\": \"Tag Version\",\n"
                    + "    \"kind\": \"DOUBLE\",\n"
                    + "    \"doubleValue\": 1.2\n"
                    + "  }]\n"
                    + "}",
                CatalogTag.class));
  }

  @Test
  public void toCatalogTag_tagWithEnumColumn_valid() {
    Tag tag = parseJson(load("single_tag_enum_column.json"), Tag.class);

    assertThat(toCatalogTag(tag))
        .isEqualTo(
            parseJson(
                "{\n"
                    + "  \"tagId\": \"projects/my-project-id/locations/us/entryGroups/@bigquery/entries/cHJvamVjdHMvZ29vZ2xlLmNvbTphbmFudGQvZGF0YXNldHMvTXlEYXRhU2V0L3RhYmxlcy9Nb2NrUGlpUHJvY2Vzc2Vk/tags/CUzSbDlKy_z_bF\",\n"
                    + "  \"templateId\": \"projects/my-project-id/locations/us/tagTemplates/sample_catalog_tag\",\n"
                    + "  \"templateName\": \"Testing Sample Catalog Tag\",\n"
                    + "  \"column\": \"someColumn\",\n"
                    + "  \"fields\": [{\n"
                    + "    \"fieldId\": \"primary_source\",\n"
                    + "    \"fieldName\": \"Primary Source\",\n"
                    + "    \"kind\": \"BOOL\",\n"
                    + "    \"boolValue\": true\n"
                    + "  }, {\n"
                    + "    \"fieldId\": \"business_unit\",\n"
                    + "    \"fieldName\": \"Business Unit\",\n"
                    + "    \"kind\": \"STRING\",\n"
                    + "    \"stringValue\": \"Professional Services\"\n"
                    + "  }, {\n"
                    + "    \"fieldId\": \"data_category\",\n"
                    + "    \"fieldName\": \"Data Category\",\n"
                    + "    \"kind\": \"ENUM\",\n"
                    + "    \"enumValue\": {\n"
                    + "      \"displayName\": \"PII\"\n"
                    + "    }\n"
                    + "  }]\n"
                    + "}",
                CatalogTag.class));
  }
}
