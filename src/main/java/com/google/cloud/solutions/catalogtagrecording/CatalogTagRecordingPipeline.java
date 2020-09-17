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

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.EntityTagOperationRecord;
import com.google.protobuf.Descriptors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Defines a Dataflow pipeline to DAG that read's Data Catalog AuditLogs from PubSub, extracts the
 * present Tags attached to the Entry and writes them into a BigQuery Table.
 */
@AutoValue
public abstract class CatalogTagRecordingPipeline {

  private static final Descriptors.FieldDescriptor RECONCILE_TIME_FIELD =
      EntityTagOperationRecord.getDescriptor().findFieldByName("reconcile_time");

  abstract TagRecordingPipelineOptions options();

  abstract Pipeline pipeline();

  public final void run() {
    setupPipeline();
    pipeline().run();
  }

  private void setupPipeline() {
    pipeline()
        .apply(
            "ReadAuditLogs",
            PubsubIO.readStrings().fromSubscription(options().getCatalogAuditLogsSubscription()))
        .apply("ParseLogReadCatalogTags", EntityTagOperationRecordExtractor.defaultExtractor())
        .apply(
            "WriteToBigQuery",
            BigQueryIO.<EntityTagOperationRecord>write()
                .to(options().getTagsBigqueryTable())
                .withTimePartitioning(dayPartitionOnReconcileTime())
                .withFormatFunction(recordMapper())
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .optimizedWrites());
  }

  /**
   * Returns a mapping function to convert the proto message into a TableRow structure for writing
   * in BigQuery.
   */
  private SerializableFunction<EntityTagOperationRecord, TableRow> recordMapper() {
    return options().isSnakeCaseColumnNames()
        ? ProtoToTableRowMapper.withSnakeCase()
        : ProtoToTableRowMapper.withCamelCase();
  }

  /**
   * Returns a Date-partitioning configuration for the BigQuery output table.
   */
  private TimePartitioning dayPartitionOnReconcileTime() {
    return new TimePartitioning()
        .setType("DAY")
        .setField(
            options().isSnakeCaseColumnNames()
                ? RECONCILE_TIME_FIELD.getName()
                : RECONCILE_TIME_FIELD.getJsonName());
  }

  public static Builder builder() {
    return new AutoValue_CatalogTagRecordingPipeline.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder options(TagRecordingPipelineOptions options);

    public abstract Builder pipeline(Pipeline pipeline);

    public abstract CatalogTagRecordingPipeline build();
  }
}
