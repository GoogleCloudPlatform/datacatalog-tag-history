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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Entry point to parse the CLI arguments as Pipeline options and launch the pipeline.
 */
public final class PipelineLauncher {

  public static void main(String[] args) {
    PipelineOptionsFactory.register(TagRecordingPipelineOptions.class);

    TagRecordingPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(TagRecordingPipelineOptions.class);

    CatalogTagRecordingPipeline.builder()
        .options(options)
        .pipeline(Pipeline.create(options))
        .build()
        .run();
  }
}
