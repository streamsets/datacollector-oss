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
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.CredentialValueBean;
import com.streamsets.pipeline.lib.tls.KeyStoreType;
import com.streamsets.pipeline.lib.tls.KeyStoreTypeChooserValues;

import java.util.ArrayList;
import java.util.List;

public class RemoteConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Resource URL",
      description = "Specify the SFTP/FTP/FTPS URL",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String remoteAddress;

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
      defaultValue = "EXPLICIT",
      label = "FTPS Mode",
      description = "Sets the FTPS encryption negotiation mode to either \"Explicit\" (also called FTPES) or " +
          "\"Implicit\". \"Implicit\" assumes that encryption will be used immediately, while \"Explicit\" means that " +
          "plain FTP will be used to connect and then encryption will be negotiated.",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  @ValueChooserModel(FTPSModeChooserValues.class)
  public FTPSMode ftpsMode = FTPSMode.EXPLICIT;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "PRIVATE",
      label = "FTPS Data Channel Protection Level",
      description = "Sets the FTPS data channel protection level to either \"Clear\" (equivalent of \"PROT C\") or " +
          "\"Private\" (equivalent of \"PROT P\").  \"Private\" means that the communication and data are both " +
          "encrypted, while \"Clear\" means that only the communication is encrypted.",
      displayPosition = 65,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  @ValueChooserModel(FTPSDataChannelProtectionLevelChooserValues.class)
  public FTPSDataChannelProtectionLevel ftpsDataChannelProtectionLevel = FTPSDataChannelProtectionLevel.PRIVATE;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Authentication",
      description = "The authentication method to use to login to remote server",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1"
  )
  @ValueChooserModel(AuthenticationChooserValues.class)
  public Authentication auth = Authentication.NONE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      description = "Username to use to login to the remote server",
      displayPosition = 15,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependsOn = "auth",
      triggeredByValue = {"PASSWORD", "PRIVATE_KEY"}
  )
  public CredentialValue username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Password to use to login to the remote server. If private key is specified, that is used.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependsOn = "auth",
      triggeredByValue = {"PASSWORD"}
  )
  public CredentialValue password;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "FILE",
      label = "Private Key Provider",
      description = "Provide the private key via a file or plain text",
      displayPosition = 25,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependsOn = "auth",
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
      dependsOn = "auth",
      triggeredByValue = {"PRIVATE_KEY"}
  )
  public CredentialValue privateKeyPassphrase;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Strict Host Checking",
      description = "If enabled, will only connect to the host if the host is in the known hosts file.",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#1"
  )
  public boolean strictHostChecking;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Known Hosts file",
      description = "Full path to the file that lists the host keys of all known hosts." +
          "This must be specified if the strict host checking is enabled.",
      group = "#1",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "strictHostChecking",
      triggeredByValue = "true"
  )
  public String knownHosts;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Client Certificate for FTPS",
      description = "Enable this if the FTPS Server requires mutual authentication. The client will need to provide " +
          "a keystore file containing the client certificate.",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1"
  )
  public boolean useFTPSClientCert;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      description = "Use a keystore built from a specified private key and certificate chain instead of loading from a local file",
      label = "Use Remote Keystore",
      displayPosition = 71,
      group = "#1",
      dependsOn = "useFTPSClientCert",
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
          @Dependency(configName = "useFTPSClientCert", triggeredByValues = "true"),
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
          @Dependency(configName = "useFTPSClientCert", triggeredByValues = "true"),
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
          @Dependency(configName = "useFTPSClientCert", triggeredByValues = "true"),
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
          @Dependency(configName = "useFTPSClientCert", triggeredByValues = "true"),
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
          @Dependency(configName = "useFTPSClientCert", triggeredByValues = "true"),
          @Dependency(configName = "useRemoteKeyStore", triggeredByValues = "false")
      }
  )
  public CredentialValue ftpsClientCertKeystorePassword = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ALLOW_ALL",
      label = "FTPS Truststore Provider",
      description = "Providing a Truststore allows the client to verify the FTPS Server's certificate. " +
          "\"Allow All\" will allow any certificate, skipping validation. " +
          "\"File\" will allow providing a truststore file containing the certificate. " +
          "\"Remote Truststore\" allows providing a list of trusted certificates to build the truststore" +
          "\"JVM Default\" will use the JVM's default truststore.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1"
  )
  @ValueChooserModel(FTPSTrustStoreChooserValues.class)
  public FTPSTrustStore ftpsTrustStoreProvider = FTPSTrustStore.ALLOW_ALL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "FTPS Truststore File",
      description = "Full path to the truststore file containing the server certificate",
      displayPosition = 81,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependsOn = "ftpsTrustStoreProvider",
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
      dependsOn = "ftpsTrustStoreProvider",
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
      dependsOn = "ftpsTrustStoreProvider",
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
      dependsOn = "ftpsTrustStoreProvider",
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
      group = "#0"
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
