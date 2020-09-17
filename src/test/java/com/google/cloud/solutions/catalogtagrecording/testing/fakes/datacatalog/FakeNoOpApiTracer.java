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

import com.google.api.gax.tracing.ApiTracer;
import org.threeten.bp.Duration;

public class FakeNoOpApiTracer implements ApiTracer {

  @Override
  public Scope inScope() {
    return null;
  }

  @Override
  public void operationSucceeded() {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void operationCancelled() {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void operationFailed(Throwable throwable) {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void connectionSelected(String s) {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void attemptStarted(int i) {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void attemptSucceeded() {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void attemptCancelled() {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void attemptFailed(Throwable throwable, Duration duration) {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void attemptFailedRetriesExhausted(Throwable throwable) {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void attemptPermanentFailure(Throwable throwable) {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void lroStartFailed(Throwable throwable) {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void lroStartSucceeded() {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void responseReceived() {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void requestSent() {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }

  @Override
  public void batchRequestSent(long l, long l1) {
    // Do nothing because this is a Fake and doesn't implement an actual gRPC
    // operation.
  }
}
