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

import static com.google.cloud.solutions.catalogtagrecording.TagUtility.extractEntityId;

import com.google.auto.value.AutoValue;
import com.google.cloud.datacatalog.v1beta1.stub.DataCatalogStub;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.AuditInformation;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.CatalogEntry;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.EntityTagOperationRecord;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.OperationInformation;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.text.ParseException;
import java.time.Clock;
import java.time.Instant;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Custom Transform to encapsulate parsing LogEntry and enriching the information with Tags data
 * from DataCatalog.
 */
@AutoValue
public abstract class EntityTagOperationRecordExtractor
    extends PTransform<PCollection<String>, PCollection<EntityTagOperationRecord>> {

  protected static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * Set of events which will be processed by the enricher.
   */
  private static final ImmutableSet<String> USABLE_OPERATION_METHODS =
      ImmutableSet.of("CreateTag", "UpdateTag", "DeleteTag");

  /**
   * Configurable Clock instant for Unit Testing.
   */
  public abstract Clock clock();

  /**
   * Configurable DataCatalogStub for Unit Testing.
   */
  @Nullable
  public abstract DataCatalogStub catalogStub();

  @Override
  public PCollection<EntityTagOperationRecord> expand(PCollection<String> input) {
    return input
        .apply("ExtractRecord", MapElements.via(new LogEntryToRecordMapper()))
        .apply("RetrieveCatalogTags", MapElements.via(new CatalogTagEnricher()))
        .apply("RemoveEmptyEnriched", Filter.by(new NonDefaultRecord()));
  }

  /** Filter to removes default record proto messages from upstream exceptions. */
  private static class NonDefaultRecord
      implements SerializableFunction<EntityTagOperationRecord, Boolean> {

    @Override
    public Boolean apply(EntityTagOperationRecord record) {
      return !record.equals(EntityTagOperationRecord.getDefaultInstance());
    }
  }

  /** Service to create OperationRecord by filtering LogEntries and parsing through a JsonParser. */
  private class CatalogTagEnricher
      extends SimpleFunction<EntityTagOperationRecord, EntityTagOperationRecord> {

    @Override
    public EntityTagOperationRecord apply(EntityTagOperationRecord record) {
      try (DataCatalogService service = DataCatalogService.usingStub(catalogStub())) {

        if (record.equals(EntityTagOperationRecord.getDefaultInstance())) {
          return record;
        }

        CatalogEntry enrichedEntry = service.enrichCatalogEntry(record.getEntity());

        return record.toBuilder()
            .setEntity(enrichedEntry)
            .clearTags()
            .addAllTags(service.lookUpAllTags(enrichedEntry.getEntityId()))
            .build();
      } catch (IOException ioException) {
        logger.atSevere().withCause(ioException).log("Error enriching record:%n%s", record);
      }

      return EntityTagOperationRecord.getDefaultInstance();
    }
  }

  /** Parses a LogEntry JSON string into Proto message with all available fields initialized. */
  private class LogEntryToRecordMapper extends SimpleFunction<String, EntityTagOperationRecord> {

    @SuppressWarnings("ConstantConditions") // Suppress Null checks for Proto-parser output.
    @Override
    public EntityTagOperationRecord apply(String logEntryString) {
      try {

        JsonMessageParser logEntryParser = JsonMessageParser.of(logEntryString);
        JsonMessageParser auditLogParser = logEntryParser.forSubNode("$.protoPayload");

        String catalogOperationMethod = auditLogParser.read("$.methodName");
        String methodFragment = Iterables.getLast(Splitter.on(".").split(catalogOperationMethod));

        if (!USABLE_OPERATION_METHODS.contains(methodFragment)) {
          return EntityTagOperationRecord.getDefaultInstance();
        }

        String resourceName = auditLogParser.read("$.resourceName");

        return EntityTagOperationRecord.newBuilder()
            .setReconcileTime(Timestamps.fromMillis(Instant.now(clock()).toEpochMilli()))
            .setEntity(CatalogEntry.newBuilder().setEntityId(extractEntityId(resourceName)))
            .setAuditInformation(
                AuditInformation.newBuilder()
                    .setInsertId(logEntryParser.read("$.insertId"))
                    .setActuator(auditLogParser.read("$.authenticationInfo.principalEmail"))
                    .setJobTime(Timestamps.parse(logEntryParser.read("$.timestamp")))
                    .setOperation(
                        OperationInformation.newBuilder()
                            .setResource(resourceName)
                            .setType(catalogOperationMethod)))
            .build();

      } catch (ParseException e) {
        logger.atSevere().withCause(e).log(
            "Error extracting Record from LogEntry%n%s", logEntryString);
      }

      return EntityTagOperationRecord.getDefaultInstance();
    }
  }

  public static EntityTagOperationRecordExtractor defaultExtractor() {
    return builder().build();
  }

  public static Builder builder() {
    return new AutoValue_EntityTagOperationRecordExtractor.Builder().clock(Clock.systemUTC());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder clock(Clock clock);

    public abstract Builder catalogStub(@Nullable DataCatalogStub catalogStub);

    public abstract EntityTagOperationRecordExtractor build();
  }
}
