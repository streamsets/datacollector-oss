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
package com.streamsets.pipeline.stage.config.elasticsearch;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.ConnectionVerifierDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.aws.AwsUtil;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnection;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig.SERVER_URL_CONFIG_PATH;
import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig.SSL_TRUSTSTORE_PATH_CONFIG_PATH;

@StageDef(
    version = 1,
    label = "Elasticsearch Connection Verifier",
    description = "Verifies a connection to an Elasticsearch server",
    upgraderDef = "upgrader/ElaticsearchConnectionVerifier.yaml",
    onlineHelpRefUrl = ""
)
@HideStage(HideStage.Type.CONNECTION_VERIFIER)
@ConfigGroups(ElasticsearchConnectionGroups.class)
@ConnectionVerifierDef(
    verifierType = ElasticsearchConnection.TYPE,
    connectionFieldName = "connection",
    connectionSelectionFieldName = "connectionSelection"
)
public class ElasticsearchConnectionVerifier extends ConnectionVerifier {
  private static final String ES_SERVICE_NAME = "es";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = ElasticsearchConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection"
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
  public ElasticsearchConnection connection;

  private RestClient restClient;

  @Override
  protected List<ConfigIssue> initConnection() {
    List<ConfigIssue> issues = new ArrayList<>();

    List<String> httpURIs = connection.getServerURL();

    int numHosts = httpURIs.size();
    HttpHost[] hosts = new HttpHost[numHosts];
    for (int i = 0; i < numHosts; i++) {
      hosts[i] = HttpHost.create(httpURIs.get(i));
    }
    RestClientBuilder restClientBuilder = RestClient.builder(hosts);

    try {
      List<Header> headers = new ArrayList<>();
      if (connection.useSecurity) {
        restClient = createSecureRESTClient(restClientBuilder, headers, issues);
      } else {
        restClient = restClientBuilder.build();
      }
      if (issues.isEmpty()) {
        restClient.performRequest("GET", "/", headers.toArray(new Header[0]));
      }
    } catch (final Exception ex) {
      issues.add(getContext().createConfigIssue(
          ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(), SERVER_URL_CONFIG_PATH,
          Errors.ELASTICSEARCH_43,
          httpURIs.stream()
            .map(HttpHost::create)
            .map(HttpHost::toHostString)
            .collect(Collectors.joining(",")),
          ex.getMessage(),
          ex
      ));
    }

    return issues;
  }

  private RestClient createSecureRESTClient(
      final RestClientBuilder restClientBuilder,
      final List<Header> headers,
      final List<ConfigIssue> issues
  ) {
    RestClient result = null;

    try {
      SSLContext sslContext = createSSLContext();
      switch (connection.securityConfig.securityMode) {
        case BASIC:
          headers.add(createAuthHeader());

          result = restClientBuilder.setHttpClientConfigCallback(hacb -> hacb.setSSLContext(sslContext)).build();
          break;
        case AWSSIGV4:
          HttpRequestInterceptor interceptor = createAWSInterceptor();
          result = restClientBuilder.setHttpClientConfigCallback(hacb ->
            hacb.setSSLContext(sslContext).addInterceptorLast(interceptor)
          ).build();
          break;
      }
    } catch (final IOException | KeyStoreException | NoSuchAlgorithmException | KeyManagementException | CertificateException ex) {
      issues.add(getContext().createConfigIssue(
          ElasticsearchConnectionGroups.SECURITY.name(),
          SSL_TRUSTSTORE_PATH_CONFIG_PATH,
          Errors.ELASTICSEARCH_12,
          Optional.ofNullable(ex.getMessage()).orElse("no details provided"),
          ex
      ));
    }

    return result;
  }

  private HttpRequestInterceptor createAWSInterceptor() {
    AwsRegion awsRegion = connection.securityConfig.awsRegion;
    return AwsUtil.getAwsSigV4Interceptor(
        ES_SERVICE_NAME,
        awsRegion,
        connection.securityConfig.endpoint,
        connection.securityConfig.awsAccessKeyId,
        connection.securityConfig.awsSecretAccessKey
    );
  }

  private SSLContext createSSLContext() throws
      NoSuchAlgorithmException,
      KeyStoreException,
      IOException,
      CertificateException,
      KeyManagementException {
    SSLContext sslContext = null;
    if (connection.securityConfig.enableSSL) {
      Path path = Paths.get(connection.securityConfig.sslTrustStorePath);
      String trustStorePass = connection.securityConfig.sslTrustStorePassword.get();

      KeyStore keyStore = KeyStore.getInstance("jks");
      try (InputStream is = Files.newInputStream(path)) {
        keyStore.load(is, trustStorePass.toCharArray());
      }
      sslContext = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
    } else {
      sslContext = SSLContext.getDefault();
    }
    return sslContext;
  }

  private BasicHeader createAuthHeader() {
    String securityUser = connection.securityConfig.securityUser.get();
    String securityPassword = connection.securityConfig.securityPassword.get();
    String securityData = securityUser.contains(":")
        ? securityUser
        : securityUser.concat(":").concat(securityPassword);
    return new BasicHeader(
        "Authorization",
        "Basic " + Base64.encodeBase64String(securityData.getBytes())
    );
  }

  @Override
  protected void destroyConnection() {
    if (restClient != null) {
      try {
        restClient.close();
      } catch (final IOException ex) {
        Throwables.propagate(ex);
      }
    }
  }
}
