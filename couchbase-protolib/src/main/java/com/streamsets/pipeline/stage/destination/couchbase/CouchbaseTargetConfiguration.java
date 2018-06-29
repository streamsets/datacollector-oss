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
package com.streamsets.pipeline.stage.destination.couchbase;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

public class CouchbaseTargetConfiguration {

  // Connection tab

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "localhost:8091",
    label = "URL",
    displayPosition = 10,
    description = "URL of the Couchbase NoSQL database cluster",
    group = "CONNECTION"
  )
  public String URL;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "",
    label = "Bucket",
    displayPosition = 20,
    description = "Couchbase bucket for the data",
    group = "CONNECTION"
  )
  public String bucket;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "VERSION4",
    label = "Database Version",
    description = "Version of the Couchbase NoSQL database",
    displayPosition = 30,
    group = "CONNECTION"
  )
  @ValueChooserModel(CouchbaseVersionChooserValues.class)
  public CouchbaseVersionTypes version = CouchbaseVersionTypes.VERSION5;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.CREDENTIAL,
    defaultValue = "",
    label = "Bucket Password",
    displayPosition = 40,
    description = "Bucket password for Couchbase version 4 or earlier",
    dependsOn = "version",
    triggeredByValue = "VERSION4",
    group = "CONNECTION"
  )
  public CredentialValue bucketPassword;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.CREDENTIAL,
    defaultValue = "",
    label = "Couchbase User Name",
    displayPosition = 40,
    description = "Username for the connection",
    dependsOn = "version",
    triggeredByValue = "VERSION5",
    group = "CONNECTION"
  )
  public CredentialValue userName;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.CREDENTIAL,
    defaultValue = "",
    label = "Couchbase User Password",
    displayPosition = 50,
    description = "Password for the connection",
    dependsOn = "version",
    triggeredByValue = "VERSION5",
    group = "CONNECTION"
  )
  public CredentialValue userPassword;

  // Documentation tab

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Generate Unique Document Key",
    displayPosition = 10,
    description = "Generate a unique document key if document key field cannot be set",
    group = "DOCUMENT"
  )
  public boolean generateDocumentKey;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "",
    label = "Unique Document Key Field",
    displayPosition = 20,
    description = "A field in the document/data which will be used as the unique document key in Couchbase",
    group = "DOCUMENT",
    dependsOn = "generateDocumentKey",
    triggeredByValue = "false"
  )
  public String documentKey;

}
