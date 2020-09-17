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

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.auto.value.AutoValue;
import com.google.common.flogger.GoogleLogger;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import java.util.concurrent.TimeUnit;

/**
 * A Parser for JSON using {@link JsonPath} library.
 *
 * <p>Provides services to read attributes of a structured Message.
 */
@AutoValue
public abstract class JsonMessageParser {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  abstract DocumentContext getParsedMessage();

  public abstract String getRootPath();

  /**
   * Returns the JSON message used by this parser instant.
   */
  public String getJson() {
    return getParsedMessage().jsonString();
  }

  /**
   * Returns a JSON parser for a subnode of the present Object.
   */
  public JsonMessageParser forSubNode(String subNodeKey) {
    return JsonMessageParser.builder()
        .setParsedMessage(getParsedMessage())
        .setRootPath(buildPath(subNodeKey))
        .build();
  }

  public static JsonMessageParser of(String messageJson) {
    return builder().setMessageJson(messageJson).build();
  }

  /**
   * Returns a POJO read at the provided path or null if not found.
   */
  public <T> T read(String jsonPath) {
    try {
      return getParsedMessage().read(buildPath(jsonPath));
    } catch (PathNotFoundException | NullPointerException exception) {
      logger.atInfo().withCause(exception).atMostEvery(1, TimeUnit.MINUTES).log(
          "error reading [%s]", jsonPath);
      return null;
    }
  }

  /** Returns a POJO read at the provided path or the provided default value if target is empty. */
  public <T> T readOrDefault(String jsonPath, T defaultValue) {
    return firstNonNull(read(jsonPath), defaultValue);
  }

  private String buildPath(String path) {
    checkArgument(path.startsWith("$."));
    return getRootPath() + "." + path.replaceFirst("^\\$\\.", "");
  }

  public static Builder builder() {
    return new AutoValue_JsonMessageParser.Builder().setRootPath("$");
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public Builder setMessageJson(String messageJson) {
      checkArgument(
          isNotBlank(messageJson), "JSON can't be null or empty (was: \"%s\")", messageJson);
      return setParsedMessage(JsonPath.parse(messageJson));
    }

    public abstract Builder setParsedMessage(DocumentContext newParsedMessage);

    public abstract Builder setRootPath(String newRootPath);

    abstract JsonMessageParser autoBuild();

    public JsonMessageParser build() {
      JsonMessageParser parser = autoBuild();
      checkArgument(
          isNotBlank(parser.getRootPath()),
          "RootPath can't be null or empty (was: \"%s\") default root is $",
          parser.getRootPath());

      return parser;
    }
  }
}
