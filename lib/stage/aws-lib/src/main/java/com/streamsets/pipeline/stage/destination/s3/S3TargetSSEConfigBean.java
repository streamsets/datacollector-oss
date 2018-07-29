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
package com.streamsets.pipeline.stage.destination.s3;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.lib.aws.SSEChooserValues;
import com.streamsets.pipeline.stage.lib.aws.SSEOption;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3TargetSSEConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Server-Side Encryption",
      description = "Whether or not to enable server-side encryption",
      defaultValue = "false",
      displayPosition = 10,
      group = "#0"
  )
  public boolean useSSE;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Server-Side Encryption Option",
      description = "Server-Side Encryption Option",
      defaultValue = "S3",
      displayPosition = 20,
      dependsOn = "useSSE",
      triggeredByValue = "true",
      group = "#0"
  )
  @ValueChooserModel(SSEChooserValues.class)
  public SSEOption encryption;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "AWS KMS Key ARN",
      description = "AWS KMS master encryption key that was used for the object. " +
          "The KMS key you specify in the policy must use the \"arn:aws:kms:region:acct-id:key/key-id\" format.",
      displayPosition = 30,
      dependsOn = "encryption",
      triggeredByValue = "KMS",
      group = "#0"
  )
  public CredentialValue kmsKeyId;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Encryption Context",
      description = "Set of key-value pairs that you can pass to AWS KMS",
      displayPosition = 40,
      dependsOn = "encryption",
      triggeredByValue = "KMS",
      group = "#0"
  )
  @ListBeanModel
  public List<EncryptionContextBean> encryptionContext = new ArrayList<>();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Customer Encryption Key",
      description = "256-bit, base64-encoded encryption key for Amazon S3 to use to encrypt or decrypt your data",
      displayPosition = 50,
      dependsOn = "encryption",
      triggeredByValue = "CUSTOMER",
      group = "#0"
  )
  public CredentialValue customerKey;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Customer Encryption Key MD5",
      description = "Base64-encoded 128-bit MD5 digest of the encryption key according to RFC 1321",
      displayPosition = 60,
      dependsOn = "encryption",
      triggeredByValue = "CUSTOMER",
      group = "#0"
  )
  public CredentialValue customerKeyMd5;

  public Map<String, String> resolveEncryptionContext() throws StageException {
    Map<String, String> encryptionContext = new HashMap<>();
    for(EncryptionContextBean bean : this.encryptionContext) {
      encryptionContext.put(bean.key, bean.value.get());
    }
    return encryptionContext;
  }
}
