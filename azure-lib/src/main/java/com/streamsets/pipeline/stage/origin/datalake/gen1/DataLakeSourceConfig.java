/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.datalake.gen1;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.AzureUtils;
import com.streamsets.pipeline.stage.conf.DataLakeConnectionProtocol;
import com.streamsets.pipeline.stage.conf.DataLakeSourceGroups;
import com.streamsets.pipeline.stage.destination.datalake.Errors;
import com.streamsets.pipeline.stage.destination.hdfs.HadoopConfigBean;
import com.streamsets.pipeline.stage.origin.hdfs.HdfsSourceConfigBean;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains configurations common to all Generation 2 Data Lake Connectors
 */
public class DataLakeSourceConfig extends HdfsSourceConfigBean {

  private static final Logger LOG = LoggerFactory.getLogger(DataLakeSourceConfig.class);

  private static final String ADLS_CONFIG_BEAN_PREFIX = "dataLakeConfig.";
  private static final String ADLS_CONFIG_ACCOUNT_FQDN = ADLS_CONFIG_BEAN_PREFIX + "accountFQDN";
  private static final String ADLS_CONFIG_AUTH_TOKEN_ENDPOINT = ADLS_CONFIG_BEAN_PREFIX + "authTokenEndpoint";
  private static final String ADLS_CONFIG_CLIENT_ID = ADLS_CONFIG_BEAN_PREFIX + "clientId";
  private static final String ADLS_CONFIG_CLIENT_KEY = ADLS_CONFIG_BEAN_PREFIX + "clientKey";

  private static final String ADLS_USER_AGENT_STRING_KEY = "fs.azure.user.agent.prefix";

  private static final String ADLS_GEN1_ACCESS_TOKEN_PROVIDER_KEY = "dfs.adls.oauth2.access.token.provider.type";
  private static final String ADLS_GEN1_ACCESS_TOKEN_PROVIDER_VALUE = "ClientCredential";
  private static final String ADLS_GEN1_REFRESH_URL_KEY = "dfs.adls.oauth2.refresh.url";
  private static final String ADLS_GEN1_CLIENT_ID_KEY = "dfs.adls.oauth2.client.id";
  private static final String ADLS_GEN1_CLIENT_SECRET_KEY = "dfs.adls.oauth2.credential";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Application ID",
      description = "Azure application ID",
      displayPosition = 10,
      group = "DATALAKE"
  )
  public CredentialValue clientId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "https://login.microsoftonline.com/example-example",
      label = "Auth Token Endpoint",
      description = "Azure auth token endpoint",
      displayPosition = 20,
      group = "DATALAKE"
  )
  public CredentialValue authTokenEndpoint;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "example.azuredatalakestore.net",
      label = "Account FQDN",
      description = "The fully qualified domain name of the Data Lake Storage account",
      displayPosition = 30,
      group = "DATALAKE"
  )
  public CredentialValue accountFQDN;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Application Key",
      description = "Azure application key",
      displayPosition = 40,
      group = "DATALAKE"
  )
  public CredentialValue clientKey;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Advanced Configuration",
      description = "Additional HDFS properties to pass to the underlying file system. " +
          "These properties take precedence over those defined in HDFS configuration files.",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "DATALAKE"
  )
  @ListBeanModel
  public List<HadoopConfigBean> advancedConfiguration;

  @Override
  public void init(final Stage.Context context, List<Stage.ConfigIssue> issues) {
    initHiddenDefaults();

    String accountFQDNString = resolveCredentialValue(context, this.accountFQDN, ADLS_CONFIG_ACCOUNT_FQDN, issues);
    this.hdfsUri = buildAdlUri(accountFQDNString);

    String authEndPoint = resolveCredentialValue(context, this.authTokenEndpoint, ADLS_CONFIG_AUTH_TOKEN_ENDPOINT, issues);
    String clientIdString = resolveCredentialValue(context, this.clientId, ADLS_CONFIG_CLIENT_ID, issues);
    String clientKeyString = resolveCredentialValue(context, this.clientKey, ADLS_CONFIG_CLIENT_KEY, issues);

    this.hdfsConfigs.add(new HadoopConfigBean(ADLS_GEN1_ACCESS_TOKEN_PROVIDER_KEY, ADLS_GEN1_ACCESS_TOKEN_PROVIDER_VALUE));
    this.hdfsConfigs.add(new HadoopConfigBean(ADLS_GEN1_REFRESH_URL_KEY, authEndPoint));
    this.hdfsConfigs.add(new HadoopConfigBean(ADLS_GEN1_CLIENT_ID_KEY, clientIdString));
    this.hdfsConfigs.add(new HadoopConfigBean(ADLS_GEN1_CLIENT_SECRET_KEY, clientKeyString));

    this.hdfsConfigs.addAll(this.advancedConfiguration);

    this.hdfsConfigs.add(new HadoopConfigBean(ADLS_USER_AGENT_STRING_KEY, AzureUtils.buildUserAgentString(context)));

    super.init(context, issues);
  }

  private String resolveCredentialValue(final Stage.Context context, CredentialValue credentialValue, String configName, List<Stage.ConfigIssue> issues) {
    try {
      return credentialValue.get();
    } catch (StageException e) {
      LOG.error(Errors.ADLS_15.getMessage(), e.toString(), e);
      issues.add(context.createConfigIssue(
          DataLakeSourceGroups.DATALAKE.name(),
          configName,
          Errors.ADLS_15,
          e.toString()
      ));
    }
    return null;
  }

  private void initHiddenDefaults() {
    this.hdfsUser = "";
    this.hdfsKerberos = false;
    this.hdfsConfDir = "";
    this.hdfsConfigs = new ArrayList<>();
  }

  private String buildAdlUri(String accountFQDN) {
    return DataLakeConnectionProtocol.ADL_PROTOCOL_SECURE.getProtocol() + accountFQDN;
  }

  @Override
  protected FileSystem createFileSystem() throws Exception {
    try {
      return userUgi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.newInstance(new URI(hdfsUri), hdfsConfiguration);
        }
      });
    } catch (RuntimeException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw ex;
    }
  }
}
