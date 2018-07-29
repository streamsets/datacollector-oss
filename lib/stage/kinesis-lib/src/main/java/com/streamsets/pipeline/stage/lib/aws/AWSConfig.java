/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.lib.aws;

import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.common.InterfaceAudience;
import com.streamsets.pipeline.common.InterfaceStability;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public class AWSConfig {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Access Key ID",
      displayPosition = -110,
      group = "#0"
  )
  public CredentialValue awsAccessKeyId;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Secret Access Key",
      displayPosition = -100,
      group = "#0"
  )
  public CredentialValue awsSecretAccessKey;
}
