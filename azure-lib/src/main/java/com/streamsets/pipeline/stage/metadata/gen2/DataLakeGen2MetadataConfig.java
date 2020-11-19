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

package com.streamsets.pipeline.stage.metadata.gen2;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.AzureUtils;
import com.streamsets.pipeline.stage.conf.DataLakeConnectionProtocol;
import com.streamsets.pipeline.stage.conf.connection.ADLSGen2Connection;
import com.streamsets.pipeline.stage.conf.connection.ADLSGen2Properties;
import com.streamsets.pipeline.stage.destination.datalake.Errors;
import com.streamsets.pipeline.stage.destination.datalake.Groups;
import com.streamsets.pipeline.stage.destination.hdfs.HadoopConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.streamsets.pipeline.stage.conf.connection.ADLSGen2Properties.AUTH_TOKEN_URL;
import static com.streamsets.pipeline.stage.conf.connection.ADLSGen2Properties.FS_AZURE_ACCOUNT_SHARED_KEY_PROPERTY_NAME;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Contains configurations common to all Generation 2 Data Lake Connectors
 */
public class DataLakeGen2MetadataConfig {

  private static final Logger LOG = LoggerFactory.getLogger(DataLakeGen2MetadataConfig.class);

  private static final String ADLS_CONFIG_BEAN_PREFIX = "dataLakeConfig.";
  private static final String ADLS_CONFIG_ACCOUNT_FQDN = ADLS_CONFIG_BEAN_PREFIX + "accountFQDN";
  private static final String ADLS_CONFIG_STORAGE_CONTAINER = ADLS_CONFIG_BEAN_PREFIX + "storageContainer";
  private static final String ADLS_CONFIG_CLIENT_ID = ADLS_CONFIG_BEAN_PREFIX + "clientId";
  private static final String ADLS_CONFIG_OAUTH_CLIENT_CREDS_TOKEN_PROVIDER_VALUE = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = ADLSGen2Connection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection",
      group = "#0",
      displayPosition = -500
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      dependencies = {
          @Dependency(
              configName = "connectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      }
  )
  public ADLSGen2Connection connection;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Advanced Configuration",
      description = "Additional HDFS properties to pass to the underlying file system. " +
          "These properties take precedence over those defined in HDFS configuration files.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "DATALAKE"
  )
  @ListBeanModel
  public List<HadoopConfigBean> advancedConfiguration;


  private String resolveCredentialValue(final Stage.Context context, CredentialValue credentialValue, String configName, List<Stage.ConfigIssue> issues) {
    try {
      return credentialValue.get();
    } catch (StageException e) {
      LOG.error(Errors.ADLS_15.getMessage(), e.toString(), e);
      issues.add(context.createConfigIssue(
          Groups.DATALAKE.name(),
          configName,
          Errors.ADLS_15,
          e.toString()
      ));
    }
    return null;
  }

  private String buildAbfsUri(String container, String accountFQDN) {
    String abfsProtocol = this.connection.secureConnection ?
        DataLakeConnectionProtocol.ABFS_PROTOCOL_SECURE.getProtocol() :
        DataLakeConnectionProtocol.ABFS_PROTOCOL.getProtocol();
    return abfsProtocol + container + "@" + accountFQDN;
  }

  public String getAbfsUri(final Stage.Context context, List<Stage.ConfigIssue> issues) {
    String storageContainerString = resolveCredentialValue(context,
        this.connection.storageContainer,
        ADLS_CONFIG_STORAGE_CONTAINER,
        issues
    );
    String accountFQDNString = resolveCredentialValue(context, this.connection.accountFQDN, ADLS_CONFIG_ACCOUNT_FQDN, issues);
    return buildAbfsUri(storageContainerString, accountFQDNString);
  }

  public Map<String, String> getHdfsConfigBeans(final Stage.Context context, List<Stage.ConfigIssue> issues) {
    Map<String, String> hdfsConfigs = new HashMap<>();

    String accountFQDNString = resolveCredentialValue(context, this.connection.accountFQDN, ADLS_CONFIG_ACCOUNT_FQDN, issues);
    String tenantIdString = resolveCredentialValue(context,
        this.connection.tenantId,
        "Tenant ID",
        issues
    );

    switch (this.connection.authMethod) {
      case CLIENT:
        hdfsConfigs.put(
            ADLSGen2Properties.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME,
            ADLSGen2Properties.FS_AZURE_ACCOUNT_AUTH_TYPE_DEFAULT_VALUE
        );
        hdfsConfigs.put(
            ADLSGen2Properties.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME,
            ADLS_CONFIG_OAUTH_CLIENT_CREDS_TOKEN_PROVIDER_VALUE
        );
        hdfsConfigs.put(
            ADLSGen2Properties.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT,
            String.format(AUTH_TOKEN_URL, tenantIdString)
        );
        hdfsConfigs.put(
            ADLSGen2Properties.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID,
            resolveCredentialValue(
                context,
                this.connection.clientId,
                ADLS_CONFIG_CLIENT_ID,
                issues
            )
        );
        hdfsConfigs.put(
            ADLSGen2Properties.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET,
            resolveCredentialValue(
                context,
                this.connection.clientKey,
                ADLSGen2Properties.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET,
                issues
            )
        );
        break;

      case SHARED_KEY:
        hdfsConfigs.put(
            String.format(FS_AZURE_ACCOUNT_SHARED_KEY_PROPERTY_NAME, accountFQDNString),
            resolveCredentialValue(
                context,
                this.connection.accountKey,
                String.format(FS_AZURE_ACCOUNT_SHARED_KEY_PROPERTY_NAME, accountFQDNString),
                issues
            )
        );
        break;
    }

    hdfsConfigs.put(AzureUtils.ADLS_USER_AGENT_STRING_KEY, AzureUtils.buildUserAgentString(context));

    return hdfsConfigs;
  }
}
