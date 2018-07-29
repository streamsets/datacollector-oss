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
package com.streamsets.pipeline.lib.elasticsearch;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig;
import com.streamsets.pipeline.stage.config.elasticsearch.Errors;
import com.streamsets.pipeline.stage.config.elasticsearch.Groups;
import com.streamsets.pipeline.stage.config.elasticsearch.SecurityConfig;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.sniff.ElasticsearchHostsSniffer;
import org.elasticsearch.client.sniff.HostsSniffer;
import org.elasticsearch.client.sniff.Sniffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticsearchStageDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchStageDelegate.class);
  private static final Pattern URI_PATTERN = Pattern.compile("\\S+:(\\d+)");
  private static final Pattern SECURITY_USER_PATTERN = Pattern.compile("\\S+:\\S+");

  private final Stage.Context context;
  private final ElasticsearchConfig conf;
  private RestClient restClient;
  private Sniffer sniffer;

  public ElasticsearchStageDelegate(Stage.Context context, ElasticsearchConfig conf) {
    this.context = context;
    this.conf = conf;
  }

  public List<Stage.ConfigIssue> init(String prefix, List<Stage.ConfigIssue> issues) {
    if (conf.httpUris.isEmpty()) {
      issues.add(
          context.createConfigIssue(
              Groups.ELASTIC_SEARCH.name(),
              prefix + ".httpUris",
              Errors.ELASTICSEARCH_06
          )
      );
    } else {
      for (String uri : conf.httpUris) {
        validateUri(uri, issues, prefix + ".httpUris");
      }
    }

    String securityUser = null;
    try {
      securityUser = conf.securityConfig.securityUser.get();
    } catch (StageException e) {
       issues.add(context.createConfigIssue(
          Groups.SECURITY.name(),
          SecurityConfig.CONF_PREFIX + "securityUser",
          Errors.ELASTICSEARCH_32,
           e.toString()
      ));
    }

    if (conf.useSecurity && !SECURITY_USER_PATTERN.matcher(securityUser).matches()) {
      issues.add(context.createConfigIssue(
          Groups.SECURITY.name(),
          SecurityConfig.CONF_PREFIX + "securityUser",
          Errors.ELASTICSEARCH_20
      ));
    }

    if (!issues.isEmpty()) {
      return issues;
    }

    int numHosts = conf.httpUris.size();
    HttpHost[] hosts = new HttpHost[numHosts];
    for (int i = 0; i < numHosts; i++) {
      hosts[i] = HttpHost.create(conf.httpUris.get(i));
    }
    RestClientBuilder restClientBuilder = RestClient.builder(hosts);

    try {
      if (conf.useSecurity) {
        buildSSLContext(issues, restClientBuilder);

        restClient = restClientBuilder.build();
        restClient.performRequest("GET", "/", getAuthenticationHeader(securityUser));
      } else {
        restClient = restClientBuilder.build();
        restClient.performRequest("GET", "/");
      }
    } catch (IOException e) {
      issues.add(
          context.createConfigIssue(
              Groups.ELASTIC_SEARCH.name(),
              prefix + ".httpUris",
              Errors.ELASTICSEARCH_09,
              e.toString(),
              e
          )
      );
    }

    if (!issues.isEmpty()) {
      return issues;
    }

    addSniffer(hosts);

    return issues;
  }

  public void destroy() {
    try {
      if (sniffer != null) {
        sniffer.close();
      }
      if (restClient != null) {
        restClient.close();
      }
    } catch (IOException e) {
      LOG.warn("Exception thrown while closing REST client: " + e);
    }
  }

  public Response performRequest(
      String method,
      String endpoint,
      Map<String, String> params,
      HttpEntity entity,
      Header... headers
  ) throws IOException {
    return restClient.performRequest(method, endpoint, params, entity, headers);
  }

  private void addSniffer(HttpHost[] hosts) {
    if (conf.clientSniff) {
      switch (hosts[0].getSchemeName()) {
        case "http":
          sniffer = Sniffer.builder(restClient).build();
          break;
        case "https":
          HostsSniffer hostsSniffer = new ElasticsearchHostsSniffer(
              restClient,
              ElasticsearchHostsSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT,
              ElasticsearchHostsSniffer.Scheme.HTTPS
          );
          sniffer = Sniffer.builder(restClient).setHostsSniffer(hostsSniffer).build();
          break;
        default:
          // unsupported scheme. do nothing.
      }
    }
  }

  private void buildSSLContext(List<Stage.ConfigIssue> issues, RestClientBuilder restClientBuilder) throws IOException {
    try {
      final SSLContext sslcontext;
      final String keyStorePath = conf.securityConfig.sslTrustStorePath;
      if (StringUtils.isEmpty(keyStorePath)) {
        sslcontext = SSLContext.getDefault();
      } else {
        String keyStorePass = null;
        try {
          keyStorePass = conf.securityConfig.sslTrustStorePassword.get();
        } catch (StageException e) {
           issues.add(
              context.createConfigIssue(
                  Groups.ELASTIC_SEARCH.name(),
                  SecurityConfig.CONF_PREFIX + "sslTrustStorePassword",
                  Errors.ELASTICSEARCH_31,
                  e.toString()
              )
          );
        }

        if (StringUtils.isEmpty(keyStorePass)) {
          issues.add(
              context.createConfigIssue(
                  Groups.ELASTIC_SEARCH.name(),
                  SecurityConfig.CONF_PREFIX + "sslTrustStorePassword",
                  Errors.ELASTICSEARCH_10
              )
          );
        }
        Path path = Paths.get(keyStorePath);
        if (!Files.exists(path)) {
          issues.add(
              context.createConfigIssue(
                  Groups.ELASTIC_SEARCH.name(),
                  SecurityConfig.CONF_PREFIX + "sslTrustStorePath",
                  Errors.ELASTICSEARCH_11,
                  keyStorePath
              )
          );
        }
        KeyStore keyStore = KeyStore.getInstance("jks");
        try (InputStream is = Files.newInputStream(path)) {
          keyStore.load(is, keyStorePass.toCharArray());
        }
        sslcontext = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
      }
      restClientBuilder.setHttpClientConfigCallback(
          new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
              return httpClientBuilder.setSSLContext(sslcontext);
            }
          }
      );
    } catch (KeyStoreException | NoSuchAlgorithmException | KeyManagementException | CertificateException e) {
      issues.add(
          context.createConfigIssue(
              Groups.ELASTIC_SEARCH.name(),
              SecurityConfig.CONF_PREFIX + "sslTrustStorePath",
              Errors.ELASTICSEARCH_12,
              e.toString(),
              e
          )
      );
    }
  }

  private void validateUri(String uri, List<Stage.ConfigIssue> issues, String configName) {
    Matcher matcher = URI_PATTERN.matcher(uri);
    if (!matcher.matches()) {
      issues.add(
          getContext().createConfigIssue(
              Groups.ELASTIC_SEARCH.name(),
              configName,
              Errors.ELASTICSEARCH_07,
              uri
          )
      );
    } else {
      int port = Integer.parseInt(matcher.group(1));
      if (port < 0 || port > 65535) {
        issues.add(
            getContext().createConfigIssue(
                Groups.ELASTIC_SEARCH.name(),
                configName,
                Errors.ELASTICSEARCH_08,
                port
            )
        );
      }
    }
  }

  public Header[] getAuthenticationHeader(String securityUser) {
    if (!conf.useSecurity) {
      return new Header[0];
    }

    // Credentials are in form of "username:password".
    byte[] credentials = securityUser.getBytes();
    return Collections.singletonList(new BasicHeader(
        "Authorization",
        "Basic " + Base64.encodeBase64String(credentials)
    )).toArray(new Header[1]);
  }

  private Stage.Context getContext() {
    return context;
  }
}
