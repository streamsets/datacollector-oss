/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.processor.crypto;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.credential.CredentialValue;

public class AmazonKMSConfig {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Access Key ID",
      dependencies = {
          @Dependency(configName = "masterKeyProvider^", triggeredByValues = "AWS_KMS")
      },
      displayPosition = 50,
      group = "#0"
  )
  public CredentialValue accessKeyId;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Secret Access Key",
      dependencies = {
          @Dependency(configName = "masterKeyProvider^", triggeredByValues = "AWS_KMS")
      },
      displayPosition = 51,
      group = "#0"
  )
  public CredentialValue secretAccessKey;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "KMS Key ARN",
      description = "ARN for encryption key. If in the same region as default, a bare key ID may be used.",
      dependencies = {
          @Dependency(configName = "masterKeyProvider^", triggeredByValues = "AWS_KMS")
      },
      displayPosition = 60,
      group = "#0"
  )
  public String keyArn;
}
