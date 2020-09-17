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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Adaptor to convert a Proto message into a BigQuery TableRow by parsing the proto as a JSON
 * string. It uses the {@code snakeCase} option to define the field naming convention to use for
 * converting a Proto into JSON.
 */
public final class ProtoToTableRowMapper<T extends Message>
    implements SerializableFunction<T, TableRow> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final boolean snakeCase;
  private final ObjectReader tableRowJsonReader;

  /**
   * Constructs an Adaptor instance with a given field naming convention.
   *
   * @param snakeCase set {@code true} to output {@code snake_case} formatted Column names in
   *                  TableRow, if set as {@code false} outputs {@code lowerCamelCase} formatted
   *                  column names.
   */
  private ProtoToTableRowMapper(boolean snakeCase) {
    this.snakeCase = snakeCase;
    this.tableRowJsonReader = new ObjectMapper().readerFor(TableRow.class);
  }

  /**
   * Returns a snake_case format based instance.
   */
  public static <T extends Message> ProtoToTableRowMapper<T> withSnakeCase() {
    return new ProtoToTableRowMapper<>(true);
  }

  /**
   * Returns a lowerCamelCase format based instance.
   */
  public static <T extends Message> ProtoToTableRowMapper<T> withCamelCase() {
    return new ProtoToTableRowMapper<>(false);
  }

  private JsonFormat.Printer protoJsonPrinter() {
    JsonFormat.Printer jsonPrinter = JsonFormat.printer().omittingInsignificantWhitespace();
    return (snakeCase) ? jsonPrinter.preservingProtoFieldNames() : JsonFormat.printer();
  }

  @Override
  public TableRow apply(T record) {
    try {
      return tableRowJsonReader.readValue(protoJsonPrinter().print(record));
    } catch (IOException ioException) {
      logger.atSevere().withCause(ioException).log(
          "Unable to convert record to TableRow:%n%s", record);
    }

    return new TableRow();
  }
}
