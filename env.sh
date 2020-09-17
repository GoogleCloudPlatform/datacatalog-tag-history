#!/bin/bash
#
# Copyright 2020 The Data Catalog Tag History Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Setup the GCP project to use for this tutorial
export PROJECT_ID="google.com:anantd"

# The BigQuery region to use for Tags table
export BIGQUERY_REGION="us"

# The name of the BigQuery Dataset to create the Tag records table
export DATASET_ID="catalog_dumper"

# The name of the BigQuery table for Tag records
export TABLE_ID="CamelCaseEntryLogRecord"
#"EntityTagOperationRecords"

# The Compute region to use for running Dataflow jobs and create a temporary storage bucket
export REGION_ID="us-central1"

# define the bucket id
export TEMP_GCS_BUCKET="temp-tags-dumper"

# define the name of the Pub/Sub log sink in Cloud Logging
export LOGS_SINK_NAME="datacatalog-audit-pubsub"

#define Pub/Sub topic for receiving AuditLog events
export LOGS_SINK_TOPIC_ID="catalog-audit-log-sink"

# define the subscription id
export LOGS_SUBSCRIPTION_ID="catalog-tags-dumper"

# name of the service account to use (not the email address)
export TAG_HISTORY_SERVICE_ACCOUNT="tag-history-collector"
export TAG_HISTORY_SERVICE_ACCOUNT_EMAIL="${TAG_HISTORY_SERVICE_ACCOUNT}@$(echo $PROJECT_ID | awk -F':' '{print $2"."$1}' | sed 's/^\.//').iam.gserviceaccount.com"
