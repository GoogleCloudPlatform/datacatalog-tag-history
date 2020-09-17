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

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.datacatalog.v1beta1.TagField;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.CatalogTag;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.CatalogTagField;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.CatalogTagField.CatalogTagFieldKinds;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.CatalogTagField.EnumValue;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;

/**
 * Adaptor to convert a Data Catalog's {@link Tag} into {@link CatalogTag} defined in {@code
 * proto/tag_recording_messages.proto} for definitions. The transformation is required to store a
 * dynamic key-value pair as a repeated record in BigQuery table.
 */
public final class CatalogTagConverter {

  /**
   * Converts the Key-Value pair of the Tag fields into a repeated record for storing in BigQuery.
   *
   * @param tag the Data Catalog's representation of an Entry's Tag.
   * @return A BigQuery representation as flattened repeated record for the same Tag.
   */
  public static CatalogTag toCatalogTag(Tag tag) {
    CatalogTag.Builder builder =
        CatalogTag.newBuilder()
            .setTagId(tag.getName())
            .setTemplateId(tag.getTemplate())
            .setTemplateName(tag.getTemplateDisplayName())
            .addAllFields(
                Optional.ofNullable(tag.getFieldsMap())
                    .orElseGet(ImmutableMap::of)
                    .entrySet()
                    .stream()
                    .map(CatalogTagFieldConverter::convert)
                    .collect(toImmutableSet()));

    if (tag.getColumn() != null) {
      builder.setColumn(tag.getColumn());
    }

    return builder.build();
  }

  /**
   * Helper class to transform Data-type specific TagFields.
   */
  private static class CatalogTagFieldConverter {

    static CatalogTagField convert(Map.Entry<String, TagField> entry) {
      return convert(entry.getKey(), entry.getValue());
    }

    static CatalogTagField convert(String fieldId, TagField tagField) {
      CatalogTagField.Builder builder =
          CatalogTagField.newBuilder().setFieldId(fieldId).setFieldName(tagField.getDisplayName());

      switch (tagField.getKindCase()) {
        case BOOL_VALUE:
          builder.setBoolValue(tagField.getBoolValue());
          builder.setKind(CatalogTagFieldKinds.BOOL);
          break;

        case DOUBLE_VALUE:
          builder.setDoubleValue(tagField.getDoubleValue());
          builder.setKind(CatalogTagFieldKinds.DOUBLE);
          break;

        case STRING_VALUE:
          builder.setStringValue(tagField.getStringValue());
          builder.setKind(CatalogTagFieldKinds.STRING);
          break;

        case TIMESTAMP_VALUE:
          builder.setTimestampValue(tagField.getTimestampValue());
          builder.setKind(CatalogTagFieldKinds.TIMESTAMP);
          break;

        case ENUM_VALUE:
          builder.setEnumValue(
              EnumValue.newBuilder()
                  .setDisplayName(tagField.getEnumValue().getDisplayName())
                  .build());
          builder.setKind(CatalogTagFieldKinds.ENUM);
          break;

        case KIND_NOT_SET:
          throw new IllegalStateException("Unknown KIND of tagField");
      }

      return builder.build();
    }

    private CatalogTagFieldConverter() {}
  }

  private CatalogTagConverter() {}
}
