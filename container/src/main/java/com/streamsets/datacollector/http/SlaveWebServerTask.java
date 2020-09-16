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
package com.streamsets.datacollector.http;

import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.nio.file.Paths;
import java.util.Set;

public class SlaveWebServerTask extends DataCollectorWebServerTask {

  private Configuration conf;
  static final String HTTPS_WORKER_KEYSTORE_PATH = "https.cluster.keystore.path";
  private static final String HTTPS_WORKER_KEYSTORE_PATH_DEFAULT = "/opt/security/jks/sdc-keystore.jks";
  static final String HTTPS_WORKER_KEYSTORE_PASSWORD = "https.cluster.keystore.password";
  private static final String HTTPS_WORKER_KEYSTORE_PASSWORD_DEFAULT = "${file(\"/opt/security/jks/keystore-password"
      + ".txt\")}";
  static final String HTTPS_WORKER_TRUSTSTORE_PATH = "https.cluster.truststore.path";
  private static final String HTTPS_WORKER_TRUSTSTORE_PATH_DEFAULT = null;
  static final String HTTPS_WORKER_TRUSTSTORE_PASSWORD = "https.cluster.truststore.password";
  private static final String HTTPS_WORKER_TRUSTSTORE_PASSWORD_DEFAULT = null;

  private static final Logger LOG = LoggerFactory.getLogger(SlaveWebServerTask.class);

  @Inject
  public SlaveWebServerTask(
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      Configuration conf,
      Activation activation,
      Set<ContextConfigurator> contextConfigurators,
      Set<WebAppProvider> webAppProviders,
      UserGroupManager userGroupManager,
      AsterContext asterContext
  ) {
    super(buildInfo, runtimeInfo, conf, activation, contextConfigurators, webAppProviders, userGroupManager, asterContext);
    this.conf = conf;
  }

  @Override
  protected SslContextFactory.Server createSslContextFactory() {
    SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
    File keyStore = getWorkerHttpsKeystore();
    if (!keyStore.exists()) {
      throw new IllegalStateException(Utils.format("Keystore file '{}' does not exist on worker", keyStore.getPath()));
    }
    String password = conf.get(HTTPS_WORKER_KEYSTORE_PASSWORD, HTTPS_WORKER_KEYSTORE_PASSWORD_DEFAULT).trim();
    sslContextFactory.setKeyStorePath(keyStore.getPath());
    sslContextFactory.setKeyStorePassword(password);
    sslContextFactory.setKeyManagerPassword(password);
    File trustStoreFile = getWorkerHttpsTruststore();
    if (trustStoreFile != null) {
      if (trustStoreFile.exists()) {
        sslContextFactory.setTrustStorePath(trustStoreFile.getPath());
        String truststorePassword = Utils.checkNotNull(
            conf.get(HTTPS_WORKER_TRUSTSTORE_PASSWORD, HTTPS_WORKER_TRUSTSTORE_PASSWORD_DEFAULT),
            HTTPS_WORKER_TRUSTSTORE_PASSWORD
        );
        sslContextFactory.setTrustStorePassword(truststorePassword.trim());
      } else {
        throw new IllegalStateException(Utils.format(
            "Truststore file: '{}' doesn't exist on worker",
            trustStoreFile.getPath()
        ));
      }
    }
    return sslContextFactory;
  }

  @Override
  protected String getComponentId(Configuration appConfiguration) {
    return getRuntimeInfo().getMasterSDCId();
  }

  private File getWorkerHttpsKeystore() {
    final String httpsKeystorePath = conf.get(HTTPS_WORKER_KEYSTORE_PATH, HTTPS_WORKER_KEYSTORE_PATH_DEFAULT);
    if (httpsKeystorePath == null || httpsKeystorePath.trim().isEmpty()) {
      throw new IllegalStateException(Utils.format("Keystore config: '{}' is not set on worker",
          HTTPS_WORKER_KEYSTORE_PATH
      ));
    } else if (Paths.get(httpsKeystorePath).isAbsolute()) {
      return new File(httpsKeystorePath).getAbsoluteFile();
    } else {
      throw new IllegalStateException(Utils.format(
          "Path to worker keystore file: '{}' should be in absolute " + "location",
          httpsKeystorePath
      ));
    }
  }

  private File getWorkerHttpsTruststore() {
    final String httpsTruststorePath = conf.get(HTTPS_WORKER_TRUSTSTORE_PATH, HTTPS_WORKER_TRUSTSTORE_PATH_DEFAULT);
    if (httpsTruststorePath == null || httpsTruststorePath.trim().isEmpty()) {
      LOG.info(Utils.format(
          "Truststore config '{}' is not set on worker, will pickup truststore from " +
              "$JAVA_HOME/jre/lib/security/cacerts",
          HTTPS_WORKER_TRUSTSTORE_PATH
      ));
      return null;
    } else if (Paths.get(httpsTruststorePath).isAbsolute()) {
      return new File(httpsTruststorePath).getAbsoluteFile();
    } else {
      throw new IllegalStateException(Utils.format(
          "Path to worker truststore file: '{}' should be in absolute " + "location",
          httpsTruststorePath
      ));
    }
  }

}
