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

import static com.google.cloud.solutions.catalogtagrecording.ProtoJsonConverter.parseJson;
import static com.google.cloud.solutions.catalogtagrecording.testing.TestResourceLoader.load;

import com.google.cloud.datacatalog.v1beta1.stub.DataCatalogStub;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.EntityTagOperationRecord;
import com.google.cloud.solutions.catalogtagrecording.testing.PCollectionSatisfies;
import com.google.cloud.solutions.catalogtagrecording.testing.fakes.datacatalog.FakeDataCatalogStub;
import com.google.common.collect.ImmutableList;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class EntityTagOperationRecordExtractorTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  private final Clock fixedClock =
      Clock.fixed(ZonedDateTime.parse("2020-09-11T10:52:01.789Z").toInstant(), ZoneOffset.UTC);

  @Test
  public void entityTagOperationRecord_serializable() {
    PCollection<EntityTagOperationRecord> records =
        p.apply(
            Create.of(
                parseJson(
                    load("create_tag_operation_record.json"), EntityTagOperationRecord.class)));

    PAssert.thatSingleton(records)
        .isEqualTo(
            parseJson(load("create_tag_operation_record.json"), EntityTagOperationRecord.class));

    p.run();
  }

  @Test
  public void expand_createMessage_valid() {
    DataCatalogStub fakeCatalogStub =
        FakeDataCatalogStub.buildWithTestData(
            ImmutableList.of("datacatalog-objects/TableA_entry.json"),
            ImmutableList.of(
                "datacatalog-objects/TableA_sample_template_tag.json",
                "datacatalog-objects/TableA_pii_tag.json"));

    PCollection<EntityTagOperationRecord> records =
        p.apply(Create.of(load("create_tag_request.json")))
            .apply(
                EntityTagOperationRecordExtractor.builder()
                    .clock(fixedClock)
                    .catalogStub(fakeCatalogStub)
                    .build());

    PAssert.thatSingleton(records)
        .satisfies(
            PCollectionSatisfies.singleton(
                parseJson(
                    load("create_tag_operation_record.json"), EntityTagOperationRecord.class)));

    p.run();
  }

  @Test
  public void expand_deleteMessage_valid() {
    DataCatalogStub fakeCatalogStub =
        FakeDataCatalogStub.buildWithTestData(
            ImmutableList.of("datacatalog-objects/TableB_entry.json"),
            ImmutableList.of("datacatalog-objects/TableB_pii_tag.json"));

    PCollection<EntityTagOperationRecord> records =
        p.apply(Create.of(load("delete_tag_request_1.json")))
            .apply(
                EntityTagOperationRecordExtractor.builder()
                    .clock(fixedClock)
                    .catalogStub(fakeCatalogStub)
                    .build());

    PAssert.thatSingleton(records)
        .satisfies(
            PCollectionSatisfies.singleton(
                parseJson(
                    load("delete_tag_operation_record.json"), EntityTagOperationRecord.class)));

    p.run();
  }

  @Test
  public void expand_multipleEvents_validSequence() {
    DataCatalogStub fakeCatalogStub =
        FakeDataCatalogStub.buildWithTestData(
            ImmutableList.of(
                "datacatalog-objects/TableA_entry.json", "datacatalog-objects/TableB_entry.json"),
            ImmutableList.of(
                "datacatalog-objects/TableA_sample_template_tag.json",
                "datacatalog-objects/TableA_pii_tag.json",
                "datacatalog-objects/TableB_pii_tag.json"));

    PCollection<EntityTagOperationRecord> records =
        p.apply(Create.of(load("create_tag_request.json"), load("delete_tag_request_1.json")))
            .apply(
                EntityTagOperationRecordExtractor.builder()
                    .clock(fixedClock)
                    .catalogStub(fakeCatalogStub)
                    .build());

    PAssert.that(records)
        .satisfies(
            PCollectionSatisfies.expectedSet(
                parseJson(load("create_tag_operation_record.json"), EntityTagOperationRecord.class),
                parseJson(
                    load("delete_tag_operation_record.json"), EntityTagOperationRecord.class)));

    p.run();
  }

  @Test
  public void expand_threeEventsWithOneInvalidEvent_invalidFilteredOut() {
    DataCatalogStub fakeCatalogStub =
        FakeDataCatalogStub.buildWithTestData(
            ImmutableList.of(
                "datacatalog-objects/TableA_entry.json", "datacatalog-objects/TableB_entry.json"),
            ImmutableList.of(
                "datacatalog-objects/TableA_sample_template_tag.json",
                "datacatalog-objects/TableA_pii_tag.json",
                "datacatalog-objects/TableB_pii_tag.json"));

    PCollection<EntityTagOperationRecord> records =
        p.apply(
            Create.of(
                load("create_tag_request.json"),
                load("catalog_test_permissions_audit_log.json"),
                load("delete_tag_request_1.json")))
            .apply(
                EntityTagOperationRecordExtractor.builder()
                    .clock(fixedClock)
                    .catalogStub(fakeCatalogStub)
                    .build());

    PAssert.that(records)
        .satisfies(
            PCollectionSatisfies.expectedSet(
                parseJson(load("create_tag_operation_record.json"), EntityTagOperationRecord.class),
                parseJson(
                    load("delete_tag_operation_record.json"), EntityTagOperationRecord.class)));

    p.run();
  }

  @Test
  public void expand_invalidEvent_empty() {
    DataCatalogStub fakeCatalogStub =
        FakeDataCatalogStub.buildWithTestData(
            ImmutableList.of(
                "datacatalog-objects/TableA_entry.json", "datacatalog-objects/TableB_entry.json"),
            ImmutableList.of(
                "datacatalog-objects/TableA_sample_template_tag.json",
                "datacatalog-objects/TableA_pii_tag.json",
                "datacatalog-objects/TableB_pii_tag.json"));

    PCollection<EntityTagOperationRecord> records =
        p.apply(Create.of(load("catalog_test_permissions_audit_log.json")))
            .apply(
                EntityTagOperationRecordExtractor.builder()
                    .clock(fixedClock)
                    .catalogStub(fakeCatalogStub)
                    .build());

    PAssert.that(records).empty();

    p.run();
  }
}
