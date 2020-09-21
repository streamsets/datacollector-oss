/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.transformer.config;

import com.streamsets.datacollector.config.PipelineGroups;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.stage.lib.aws.AwsRegionChooserValues;
import com.streamsets.pipeline.lib.aws.SseOption;

import static com.streamsets.pipeline.stage.common.emr.EMRClusterConnection.JOB_FLOW_ROLE_DEFAULT;
import static com.streamsets.pipeline.stage.common.emr.EMRClusterConnection.SERVICE_ROLE_DEFAULT;

/**
 * This will contain all the Transformer-specific, non-connection fields (i.e. those not found in
 * {@link com.streamsets.pipeline.stage.common.emr.EMRClusterConnection}).
 *
 * ALL config fields need to be made dependent on Transformer+EMR (see existing configs for an example)
 */
public class AmazonEMRConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Enable Server-Side Encryption",
    description = "Server-Side Encryption",
    defaultValue = "NONE",
    displayPosition = 50100,
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = PipelineGroups.CLUSTER_GROUP_NAME,
    dependsOn = "^clusterConfig.clusterType",
    triggeredByValue = "EMR"
  )
  @ValueChooserModel(SseOptionChooserValues.class)
  public SseOption encryption;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.CREDENTIAL,
    label = "AWS KMS Key ARN",
    description = "AWS KMS master encryption key that was used for the object. " +
                    "The KMS key you specify in the policy must use the \"arn:aws:kms:region:acct-id:key/key-id\" format.",
    displayPosition = 50200,
    dependsOn = "encryption",
    triggeredByValue = "KMS",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    group = PipelineGroups.CLUSTER_GROUP_NAME
  )
  public CredentialValue kmsKeyId;
}
