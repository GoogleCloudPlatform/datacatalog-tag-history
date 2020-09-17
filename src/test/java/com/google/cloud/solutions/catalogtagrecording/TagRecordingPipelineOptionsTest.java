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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.base.Splitter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class TagRecordingPipelineOptionsTest {

  @Test
  public void getCatalogAuditLogsSubscription_flagAbsent_throwsException() {
    IllegalArgumentException iaex =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> optionsFromArgString("--tagsBigqueryTable=project-id:dataset.table"));

    assertThat(iaex).hasMessageThat().contains("--catalogAuditLogsSubscription");
  }

  @Test
  public void getCatalogAuditLogsSubscription_valid() {
    assertThat(
        optionsFromArgString(
            "--catalogAuditLogsSubscription=projects/my-subscription "
                + "--tagsBigqueryTable=project-id:dataset.table")
            .getCatalogAuditLogsSubscription())
        .isEqualTo("projects/my-subscription");
  }

  @Test
  public void getTagsBigqueryTable_flagAbsent_throwsException() {
    IllegalArgumentException iaex =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> optionsFromArgString("--catalogAuditLogsSubscription=projects/my-subscription"));

    assertThat(iaex).hasMessageThat().contains("--tagsBigqueryTable");
  }

  @Test
  public void getTagsBigqueryTable_valid() {
    assertThat(
            optionsFromArgString(
                "--catalogAuditLogsSubscription=projects/my-subscription "
                        + "--tagsBigqueryTable=project-id:dataset.table")
                .getTagsBigqueryTable())
        .isEqualTo("project-id:dataset.table");
  }

  private static TagRecordingPipelineOptions optionsFromArgString(String argsString) {
    return PipelineOptionsFactory.fromArgs(
        Splitter.on(' ').trimResults().splitToList(argsString).toArray(new String[0]))
        .withValidation()
        .as(TagRecordingPipelineOptions.class);
  }

  @Test
  public void isSnakeCase_flagAbsent_false() {
    assertThat(
        optionsFromArgString(
            "--catalogAuditLogsSubscription=projects/my-subscription "
                + "--tagsBigqueryTable=project-id:dataset.table")
            .isSnakeCaseColumnNames())
        .isFalse();
  }

  @Test
  public void isSnakeCase_flagFalse_false() {
    assertThat(
        optionsFromArgString(
            "--catalogAuditLogsSubscription=projects/my-subscription "
                + "--tagsBigqueryTable=project-id:dataset.table "
                + "--snakeCaseColumnNames=false")
            .isSnakeCaseColumnNames())
        .isFalse();
  }

  @Test
  public void isSnakeCase_flagPresent_true() {
    assertThat(
        optionsFromArgString(
            "--catalogAuditLogsSubscription=projects/my-subscription "
                + "--tagsBigqueryTable=project-id:dataset.table "
                + "--snakeCaseColumnNames")
            .isSnakeCaseColumnNames())
        .isTrue();
  }

  @Test
  public void isSnakeCase_flagTrue_true() {
    assertThat(
        optionsFromArgString(
            "--catalogAuditLogsSubscription=projects/my-subscription "
                + "--tagsBigqueryTable=project-id:dataset.table "
                + "--snakeCaseColumnNames=true")
            .isSnakeCaseColumnNames())
        .isTrue();
  }
}
