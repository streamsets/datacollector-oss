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
package com.streamsets.pipeline.lib.remote;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.CredentialValueBean;
import com.streamsets.pipeline.lib.tls.KeyStoreType;
import com.streamsets.pipeline.lib.tls.KeyStoreTypeChooserValues;
import com.streamsets.pipeline.stage.connection.remote.RemoteConnection;

import java.util.ArrayList;
import java.util.List;

public class RemoteConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = RemoteConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection",
      group = "#0",
      displayPosition = 5
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
  public RemoteConnection connection;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Path Relative to User Home Directory",
      description = "If checked, the path is resolved relative to the logged in user's home directory, " +
          "if a username is entered in the Credentials tab or in the URL.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public boolean userDirIsRoot = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Create Path",
      description = "If checked, the path will be created if it does not exist",
      displayPosition = 30,
      group = "#0"
  )
  public boolean createPathIfNotExists;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "FILE",
      label = "Private Key Provider",
      description = "Provide the private key via a file or plain text",
      displayPosition = 25,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependsOn = "connection.credentials.auth",
      triggeredByValue = {"PRIVATE_KEY"}
  )
  @ValueChooserModel(PrivateKeyProviderChooserValues.class)
  public PrivateKeyProvider privateKeyProvider = PrivateKeyProvider.FILE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Private Key File",
      description = "Private key file to use to login to the remote server.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependsOn = "privateKeyProvider",
      triggeredByValue = {"FILE"}
  )
  public String privateKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Private Key",
      description = "Private key to use to login to the remote server",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependsOn = "privateKeyProvider",
      triggeredByValue = {"PLAIN_TEXT"}
  )
  public CredentialValue privateKeyPlainText;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Private Key Passphrase",
      description = "Passphrase to use to decrypt the private key.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependsOn = "connection.credentials.auth",
      triggeredByValue = {"PRIVATE_KEY"}
  )
  public CredentialValue privateKeyPassphrase;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      description = "Use a keystore built from a specified private key and certificate chain instead of loading from a local file",
      label = "Use Remote Keystore",
      displayPosition = 71,
      group = "#1",
      dependsOn = "connection.credentials.useFTPSClientCert",
      triggeredByValue = "true"
  )
  public boolean useRemoteKeyStore;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "FTPS Client Certificate Keystore File",
      description = "Full path to the keystore file containing the client certificate",
      displayPosition = 72,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependencies = {
          @Dependency(configName = "connection.credentials.useFTPSClientCert", triggeredByValues = "true"),
          @Dependency(configName = "useRemoteKeyStore", triggeredByValues = "false")
      }
  )
  public String ftpsClientCertKeystoreFile;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      description = "Private Key used in the keystore",
      label = "Private Key",
      displayPosition = 73,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependencies = {
          @Dependency(configName = "connection.credentials.useFTPSClientCert", triggeredByValues = "true"),
          @Dependency(configName = "useRemoteKeyStore", triggeredByValues = "true")
      }
  )
  public CredentialValue ftpsPrivateKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      description = "Certificate chain used in the keystore",
      label = "Certificate Chain",
      displayPosition = 74,
      group = "#1",
      dependencies = {
          @Dependency(configName = "connection.credentials.useFTPSClientCert", triggeredByValues = "true"),
          @Dependency(configName = "useRemoteKeyStore", triggeredByValues = "true")
      }
  )
  @ListBeanModel
  public List<CredentialValueBean> ftpsCertificateChain = new ArrayList<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JKS",
      label = "FTPS Client Certificate Keystore Type",
      description = "The FTPS Client Certificate Keystore type",
      displayPosition = 74,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependencies = {
          @Dependency(configName = "connection.credentials.useFTPSClientCert", triggeredByValues = "true"),
          @Dependency(configName = "useRemoteKeyStore", triggeredByValues = "false")
      }
  )
  @ValueChooserModel(KeyStoreTypeChooserValues.class)
  public KeyStoreType ftpsClientCertKeystoreType = KeyStoreType.JKS;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "FTPS Client Certificate Keystore Password",
      description = "The password to the FTPS Client Certificate Keystore File, if applicable.  " +
          "Using a password is highly recommended for security reasons.",
      displayPosition = 75,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependencies = {
          @Dependency(configName = "connection.credentials.useFTPSClientCert", triggeredByValues = "true"),
          @Dependency(configName = "useRemoteKeyStore", triggeredByValues = "false")
      }
  )
  public CredentialValue ftpsClientCertKeystorePassword = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "FTPS Truststore File",
      description = "Full path to the truststore file containing the server certificate",
      displayPosition = 81,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependsOn = "connection.credentials.ftpsTrustStoreProvider",
      triggeredByValue = "FILE"
  )
  public String ftpsTruststoreFile;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      description = "Trusted certificates used in the truststore",
      label = "Trusted Certificates",
      displayPosition = 82,
      group = "#1",
      dependsOn = "connection.credentials.ftpsTrustStoreProvider",
      triggeredByValue = "REMOTE_TRUSTSTORE"
  )
  @ListBeanModel
  public List<CredentialValueBean> ftpsTrustedCertificates = new ArrayList<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JKS",
      label = "FTPS Truststore Type",
      description = "The FTPS Truststore type",
      displayPosition = 83,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependsOn = "connection.credentials.ftpsTrustStoreProvider",
      triggeredByValue = "FILE"
  )
  @ValueChooserModel(KeyStoreTypeChooserValues.class)
  public KeyStoreType ftpsTruststoreType = KeyStoreType.JKS;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "FTPS Truststore Password",
      description = "The password to the FTPS Truststore file, if applicable.  " +
          "Using a password is highly recommended for security reasons.",
      displayPosition = 84,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependsOn = "connection.credentials.ftpsTrustStoreProvider",
      triggeredByValue = {"FILE"}
  )
  public CredentialValue ftpsTruststorePassword = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Disable Read Ahead Stream",
      description = "If checked, disable the read-ahead streaming functionality of the SSH client.  Disable if" +
          " experiencing problems with larger files (ex: in whole file).  Note that this will also result in" +
          " significantly reducing performance.",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      dependencies = {
          @Dependency(configName = "connection.protocol",
              triggeredByValues = "SFTP")
      }
  )
  public boolean disableReadAheadStream;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Socket Timeout",
      description = "The socket timeout in seconds. A value of 0 indicates no timeout.",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      min = 0
  )
  public int socketTimeout = 0;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Connection Timeout",
      description = "The connection timeout in seconds. A value of 0 indicates no timeout.",
      displayPosition = 111,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      min = 0
  )
  public int connectionTimeout = 0;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Data Timeout",
      description = "The data timeout in seconds. A value of 0 indicates no timeout.",
      displayPosition = 112,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      min = 0
  )
  public int dataTimeout = 0;
}
