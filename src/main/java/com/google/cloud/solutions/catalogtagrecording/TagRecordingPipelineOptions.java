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

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

public interface TagRecordingPipelineOptions extends GcpOptions {

  @Description("The Pub/Sub subscription Id to use for receiving Data Catalog Audit logs")
  @Required
  String getCatalogAuditLogsSubscription();

  void setCatalogAuditLogsSubscription(String pubsubSubscription);

  @Description("The BigQuery table Id in \"<projectId>:<datasetId>.<tableId>\" format")
  @Required
  String getTagsBigqueryTable();

  void setTagsBigqueryTable(String tableName);

  @Description(
      "The column naming convention to use: set true for snake_case and false for camelCase")
  @Default.Boolean(false)
  boolean isSnakeCaseColumnNames();

  void setSnakeCaseColumnNames(boolean snakeCaseColumnNames);
}
