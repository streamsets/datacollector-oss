/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.conf.connection;

final public class ADLSGen2Properties {

  public final static String HDFS_URL = "abfss://%s@%s.dfs.core.windows.net%s";
  public final static String AUTH_TOKEN_URL = "https://login.microsoftonline.com/%s/oauth2/token";

  public final static String FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME = "fs.azure.account.auth.type";
  public final static String FS_AZURE_ACCOUNT_AUTH_TYPE_DEFAULT_VALUE = "OAuth";
  public final static String FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME = "fs.azure.account.oauth.provider.type";
  public final static String FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT = "fs.azure.account.oauth2.client.endpoint";
  public final static String FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID = "fs.azure.account.oauth2.client.id";
  public final static String FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET = "fs.azure.account.oauth2.client.secret";
  public final static String FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT = "fs.azure.account.oauth2.msi.tenant";
  public final static String FS_AZURE_ACCOUNT_SHARED_KEY_PROPERTY_NAME = "fs.azure.account.key.%s";

}
