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

import com.google.cloud.datacatalog.v1beta1.Tag;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods to extract parts of Data Catalog Tag.
 */
public final class TagUtility {

  public static final Pattern ENTITY_TAG_MATCHER =
      Pattern.compile(
          "^(?<entryId>projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/[^/]+)");

  /**
   * Uses {@link Tag#getName()} field to extract parent.
   *
   * @param tag the DataCatalog Tag applied to an Entity.
   * @return the parent id.
   */
  public static String extractParent(Tag tag) {
    return extractEntityId(tag.getName());
  }

  /**
   * Returns the DataCatalog entryId by matching the regex
   * <pre>^(?<entryId>projects/[^/]+/locations/[^/]+/entryGroups/[^/]+/entries/[^/]+)</pre>
   */
  public static String extractEntityId(String resource) {
    Matcher matcher = ENTITY_TAG_MATCHER.matcher(resource);
    if (!matcher.find()) {
      throw new InvalidCatalogResource(resource);
    }

    return matcher.group("entryId");
  }

  /**
   * Custom Exception class to signal invalid entryId.
   */
  public static class InvalidCatalogResource extends RuntimeException {

    public InvalidCatalogResource(String resource) {
      super(
          String.format(
              "Resource format is incorrect: %s%nNeed like:%s",
              resource, ENTITY_TAG_MATCHER.pattern()));
    }
  }

  private TagUtility() {
  }
}
