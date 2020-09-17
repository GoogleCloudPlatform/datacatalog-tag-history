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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.cloud.datacatalog.v1beta1.DataCatalogClient;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.stub.DataCatalogStub;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.CatalogEntry;
import com.google.cloud.solutions.catalogtagrecording.TagRecordingMessages.CatalogTag;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * DataCatalog API wrapper to lookup an Entry using Id and other
 */
public class DataCatalogService implements AutoCloseable {
  private final DataCatalogClient dataCatalogClient;

  private DataCatalogService(DataCatalogClient dataCatalogClient) {
    this.dataCatalogClient = checkNotNull(dataCatalogClient);
  }

  /** Convenience Factory for building an instance Data Catalog service using provided Client. */
  public static DataCatalogService using(DataCatalogClient catalogClient) {
    return new DataCatalogService(catalogClient);
  }

  /**
   * Convenience Factory for building an instance Data Catalog service using provided Stub. Creates
   * using a standard client if stub is null.
   */
  public static DataCatalogService usingStub(DataCatalogStub catalogStub) throws IOException {
    return using(
        (catalogStub == null) ? DataCatalogClient.create() : DataCatalogClient.create(catalogStub));
  }

  /**
   * Returns Entry with LinkedResource field populated from DataCatalog.
   */
  public CatalogEntry enrichCatalogEntry(CatalogEntry entity) {

    return lookupEntry(entity)
        .map(
            entry ->
                CatalogEntry.newBuilder()
                    .setEntityId(entry.getName())
                    .setLinkedResource(entry.getLinkedResource())
                    .build())
        .orElse(entity);
  }

  /** Returns an entry object by looking up the EntryId through the DataCatalog API. */
  public Optional<Entry> lookupEntry(CatalogEntry entity) {
    checkNotNull(entity, "Entity can't be null");
    checkArgument(
        isNotBlank(entity.getEntityId()), "Entity Id should a valid DataCatalog entityId");

    return Optional.ofNullable(dataCatalogClient.getEntry(entity.getEntityId()));
  }

  /** Returns a BigQuery compatible representation of Tags attached to an entry. */
  public ImmutableSet<CatalogTag> lookUpAllTags(String entryId) {
    ImmutableSet.Builder<CatalogTag> entryTagsBuilder = ImmutableSet.builder();

    dataCatalogClient
        .listTags(entryId)
        .iteratePages()
        .forEach(
            listTagsPage ->
                StreamSupport.stream(listTagsPage.getValues().spliterator(), /*parallel=*/ false)
                    .map(CatalogTagConverter::toCatalogTag)
                    .forEach(entryTagsBuilder::add));

    return entryTagsBuilder.build();
  }

  @Override
  public void close() {
    dataCatalogClient.close();
  }
}
