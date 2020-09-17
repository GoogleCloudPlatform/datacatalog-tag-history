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

package com.google.cloud.solutions.catalogtagrecording.testing.fakes.datacatalog;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.PageContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.datacatalog.v1beta1.DataCatalogClient.ListTagsPagedResponse;
import com.google.cloud.datacatalog.v1beta1.ListTagsRequest;
import com.google.cloud.datacatalog.v1beta1.ListTagsResponse;
import com.google.cloud.datacatalog.v1beta1.Tag;
import com.google.cloud.solutions.catalogtagrecording.testing.fakes.FakeApiCallContext;
import com.google.cloud.solutions.catalogtagrecording.testing.fakes.FakeApiFutureBase;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import java.util.concurrent.ExecutionException;

public class FakeDataCatalogPagesListTagsResponse extends FakeApiFutureBase<ListTagsPagedResponse> {

  private final ListTagsRequest request;
  private final ImmutableCollection<Tag> tags;
  private final ApiCallContext callContext;

  public FakeDataCatalogPagesListTagsResponse(
      ListTagsRequest request, ApiCallContext callContext, ImmutableCollection<Tag> tags) {
    this.request = request;
    this.tags = tags;
    this.callContext = buildCallContext(callContext);
  }

  private static ApiCallContext buildCallContext(ApiCallContext context) {

    if (context == null) {
      return FakeApiCallContext.builder()
          .setTracer(new FakeNoOpApiTracer())
          .setExtraHeaders(ImmutableMap.of())
          .build();
    }

    return context;
  }

  @Override
  public ListTagsPagedResponse get() throws ExecutionException, InterruptedException {
    return ListTagsPagedResponse.createAsync(
        buildPageContext(), new FakeDataCatalogListTagsApiResponse(tags))
        .get();
  }

  private PageContext<ListTagsRequest, ListTagsResponse, Tag> buildPageContext() {
    return PageContext.create(
        new UnaryCallable<ListTagsRequest, ListTagsResponse>() {
          @Override
          public ApiFuture<ListTagsResponse> futureCall(
              ListTagsRequest listTagsRequest, ApiCallContext apiCallContext) {
            return new FakeDataCatalogListTagsApiResponse(tags);
          }
        },
        new FakeListTagsRequestListTagsResponseTagPagedListDescriptor(),
        request,
        callContext);
  }
}
