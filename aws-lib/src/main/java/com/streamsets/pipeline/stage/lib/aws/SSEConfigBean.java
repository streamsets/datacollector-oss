/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.HashMap;
import java.util.Map;

public class SSEConfigBean {

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
      type = ConfigDef.Type.STRING,
      label = "AWS KMS Key ID",
      description = "ID of the AWS KMS master encryption key that was used for the object",
      defaultValue = "",
      displayPosition = 30,
      dependsOn = "encryption",
      triggeredByValue = "KMS",
      group = "#0"
  )
  public String kmsKeyId;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Encryption Context",
      description = "Set of key-value pairs that you can pass to AWS KMS",
      displayPosition = 40,
      dependsOn = "encryption",
      triggeredByValue = "KMS",
      group = "#0"
  )
  public Map<String, String> encryptionContext = new HashMap<>();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Customer Encryption Key",
      description = "256-bit, base64-encoded encryption key for Amazon S3 to use to encrypt or decrypt your data",
      defaultValue = "",
      displayPosition = 50,
      dependsOn = "encryption",
      triggeredByValue = "CUSTOMER",
      group = "#0"
  )
  public String customerKey;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Customer Encryption Key MD5",
      description = "Base64-encoded 128-bit MD5 digest of the encryption key according to RFC 1321",
      defaultValue = "",
      displayPosition = 60,
      dependsOn = "encryption",
      triggeredByValue = "CUSTOMER",
      group = "#0"
  )
  public String customerKeyMd5;
}
