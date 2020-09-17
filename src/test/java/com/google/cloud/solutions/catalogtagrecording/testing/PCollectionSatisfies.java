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

package com.google.cloud.solutions.catalogtagrecording.testing;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class PCollectionSatisfies {

  @SafeVarargs
  public static <T> SerializableFunction<Iterable<T>, Void> expectedSet(
      T expected1, T... otherExpectedValues) {
    return expectedSet(
        ImmutableSet.<T>builder()
            .add(expected1)
            .addAll(Arrays.asList(otherExpectedValues))
            .build());
  }

  public static <T> SerializableFunction<Iterable<T>, Void> expectedSet(ImmutableSet<T> expected) {

    return (SerializableFunction<Iterable<T>, Void>)
        input -> {
          assertThat(input).containsExactlyElementsIn(expected);
          return null;
        };
  }

  public static <T> SerializableFunction<T, Void> singleton(T expected) {
    return (SerializableFunction<T, Void>)
        input -> {
          assertThat(input).isEqualTo(expected);
          return null;
        };
  }
}
