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
package com.streamsets.datacollector.credential.cyberark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.auth.DigestScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CyberArk Central Credential Provider implementation, using CyberArk web-services.
 */
public class WebServicesFetcher implements Fetcher {
  private static final Logger LOG = LoggerFactory.getLogger(WebServicesFetcher.class);

  public static final String APP_ID_PARAM = "AppID";
  public static final String SAFE_PARAM = "Safe";
  public static final String FOLDER_PARAM = "Folder";
  public static final String OBJECT_PARAM = "Object";
  public static final String CONNECTION_TIMEOUT_PARAM = "ConnectionTimeout";
  public static final String FAIL_REQUEST_ON_PASSWORD_CHANGE_PARAM = "FailRequestOnPasswordChange";

  public static final String URL_KEY = "ws.url";
  public static final String APP_ID_KEY = "ws.appId";
  public static final String KEYSTORE_FILE_KEY = "ws.keystoreFile";
  public static final String KEYSTORE_PASSWORD_KEY = "ws.keystorePassword";
  public static final String KEY_PASSWORD_KEY = "ws.keyPassword";
  public static final String TRUSTSTORE_FILE_KEY = "ws.truststoreFile";
  public static final String TRUSTSTORE_PASSWORD_KEY = "ws.truststorePassword";
  public static final String SUPPORTED_PROTOCOLS_KEY = "ws.supportedProtocols";
  public static final String HOSTNAME_VERIFIER_SKIP_KEY = "ws.hostnameVerifier.skip";
  public static final String MAX_CONCURRENT_CONNECTIONS_KEY = "ws.maxConcurrentConnections";
  public static final String VALIDATE_AFTER_INACTIVITY_KEY = "ws.validateAfterInactivity.millis";
  public static final String CONNECTION_TIMEOUT_KEY = "ws.connectionTimeout.millis";

  public static final String NAME_SEPARATOR_KEY = "ws.nameSeparator";

  public static final String HTTP_AUTH_TYPE_KEY = "ws.http.authentication";
  public static final String HTTP_AUTH_USER_KEY = "ws.http.authentication.user";
  public static final String HTTP_AUTH_PASSWORD_KEY = "ws.http.authentication.password";

  public static final String HTTP_AUTH_NONE = "none";
  public static final String HTTP_AUTH_BASIC = "basic";
  public static final String HTTP_AUTH_DIGEST = "digest";
  private static final Set<String> VALID_HTTP_AUTHENTICATIONS =
      ImmutableSet.of(HTTP_AUTH_NONE, HTTP_AUTH_BASIC, HTTP_AUTH_DIGEST);

  public static final String SEPARATOR_CS_PARAM = "separator";
  public static final String CONNECTION_CS_TIMEOUT_PARAM = CONNECTION_TIMEOUT_PARAM;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final int CONNECTION_TIMEOUT_DEFAULT = 10 * 1000;
  public static final int MAX_CONCURRENT_CONNECTIONS_DEFAULT = 10;
  public static final String NAME_SEPARATOR_DEFAULT = "&";
  public static final int VALIDATE_AFTER_INACTIVITY_DEFAULT = 60 * 1000;

  private Configuration config;
  private SSLConnectionSocketFactory sslConnectionSocketFactory;
  private PoolingHttpClientConnectionManager connectionManager;
  private HttpClientBuilder httpClientBuilder;

  private String cyberArkUrl;
  private String appId;
  private int connectTimeout;

  private RequestConfig requestConfig;

  private String httpAuth;
  private String httpAuthUser;
  private String httpAuthPassword;
  private CredentialsProvider credentialsProvider;
  private AuthCache authCache;

  private CloseableHttpClient httpClient;

  private String separator;

  protected File getFile(String path) {
    File file = null;
    if (!path.isEmpty()) {
      file = new File(path);
      if (!file.isAbsolute()) {
        // this is a bit of a hack, but we don't expose this dir to CredentialStores at the moment,
        // it would require an API change
        String etcDir = System.getProperty("sdc.conf.dir");
        file = new File(etcDir, path);
      }
    }
    return file;
  }

  @Override
  public void init(Configuration conf) {
    try {
      config = conf;

      // cyberark info
      cyberArkUrl = conf.get(URL_KEY, "").trim();
      if (cyberArkUrl.isEmpty()) {
        throw new RuntimeException("CyberArk 'url' configuration not specified");
      }

      appId = conf.get(APP_ID_KEY, "").trim();
      if (appId.isEmpty()) {
        throw new RuntimeException("CyberArk 'appId' configuration not specified");
      }

      if (cyberArkUrl.toLowerCase().startsWith("https:")) {
        sslConnectionSocketFactory = createSSLConnectionSocketFactory(conf);
      }

      // connection pool
      int maxConnections = conf.get(MAX_CONCURRENT_CONNECTIONS_KEY, MAX_CONCURRENT_CONNECTIONS_DEFAULT);
      int validateAfterInactivityMillis = conf.get(VALIDATE_AFTER_INACTIVITY_KEY, VALIDATE_AFTER_INACTIVITY_DEFAULT);

      // connection configs
      connectTimeout = conf.get(CONNECTION_TIMEOUT_KEY, CONNECTION_TIMEOUT_DEFAULT);

      // name encoding
      separator = conf.get(NAME_SEPARATOR_KEY, NAME_SEPARATOR_DEFAULT);

      RegistryBuilder registryBuilder =
          RegistryBuilder.<ConnectionSocketFactory>create().register("http", PlainConnectionSocketFactory.INSTANCE);
      if (getSslConnectionSocketFactory() != null) {
        registryBuilder.register("https", getSslConnectionSocketFactory());
      }
      // Create a registry of custom connection socket factories
      Registry<ConnectionSocketFactory> socketFactoryRegistry = registryBuilder.build();

      // Create a connection manager with custom configuration.
      connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);

      // Create socket configuration
      SocketConfig socketConfig = SocketConfig.custom().setTcpNoDelay(true).build();
      connectionManager.setDefaultSocketConfig(socketConfig);

      connectionManager.setMaxTotal(maxConnections);
      connectionManager.setDefaultMaxPerRoute(maxConnections);
      connectionManager.setValidateAfterInactivity(validateAfterInactivityMillis);

      requestConfig = RequestConfig.custom()
                                   .setConnectTimeout(getConnectionTimeout())
                                   .setConnectionRequestTimeout(getConnectionTimeout())
                                   .setSocketTimeout(getConnectionTimeout())
                                   .setRedirectsEnabled(false)
                                   .build();

      // HTTP authentication

      httpAuth = conf.get(HTTP_AUTH_TYPE_KEY, HTTP_AUTH_NONE);
      if (!VALID_HTTP_AUTHENTICATIONS.contains(getHttpAuth())) {
        throw new RuntimeException("Invalid HTTP authentication: " + getHttpAuth());
      }
      httpAuthUser = conf.get(HTTP_AUTH_USER_KEY, null);
      httpAuthPassword = conf.get(HTTP_AUTH_PASSWORD_KEY, null);
      if (!getHttpAuth().equals(HTTP_AUTH_NONE) && (getHttpAuthUser() == null || getHttpAuthPassword() == null)) {
        throw new RuntimeException(Utils.format("HTTP authentication '{}' requires the HTTP Auth user and password properties set",
            getHttpAuth()
        ));
      }

      switch (httpAuth) {
        case HTTP_AUTH_NONE:
          break;
        case HTTP_AUTH_BASIC: {
          UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(getHttpAuthUser(), getHttpAuthPassword());
          HttpHost targetHost = new HttpHost(getUrl());
          credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(AuthScope.ANY, credentials);
          authCache = new BasicAuthCache();
          getAuthCache().put(targetHost, new BasicScheme());
        }
        break;
        case HTTP_AUTH_DIGEST: {
          UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(getHttpAuthUser(), getHttpAuthPassword());
          HttpHost targetHost = new HttpHost(getUrl());
          credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(AuthScope.ANY, credentials);
          authCache = new BasicAuthCache();
          getAuthCache().put(targetHost, new DigestScheme());
        }
        break;
      }

      httpClientBuilder = HttpClients.custom()
                                     .setConnectionManager(getConnectionManager())
                                     .setDefaultRequestConfig(getRequestConfig())
                                     .disableCookieManagement();

      httpClient = getHttpClientBuilder().build();

    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @VisibleForTesting
  protected SSLConnectionSocketFactory createSSLConnectionSocketFactory(Configuration conf) throws Exception {
    SSLConnectionSocketFactory sslConnectionSocketFactory = null;
    // keystore
    File keyStoreFile = getFile(conf.get(KEYSTORE_FILE_KEY, ""));
    String keyStorePassword = conf.get(KEYSTORE_PASSWORD_KEY, "");
    String keyPassword = conf.get(KEY_PASSWORD_KEY, "");

    // trust store
    File trustStoreFile = getFile(conf.get(TRUSTSTORE_FILE_KEY, ""));
    String trustStorePassword = conf.get(TRUSTSTORE_PASSWORD_KEY, "");

    // Allow TLSv2 protocol only
    List<String> list =
        Splitter.on(",").trimResults().omitEmptyStrings().splitToList(conf.get(SUPPORTED_PROTOCOLS_KEY, "TLSv1.2"));
    String[] supportedProtocols = list.toArray(new String[list.size()]);

    // hostname verifier
    HostnameVerifier hostnameVerifier = SSLConnectionSocketFactory.getDefaultHostnameVerifier();
    if (conf.get(HOSTNAME_VERIFIER_SKIP_KEY, false)) {
      hostnameVerifier = new NoopHostnameVerifier();
    }
    SSLContextBuilder sslContextBuilder = null;
    if (keyStoreFile != null) {
      sslContextBuilder =
          SSLContexts.custom().loadKeyMaterial(keyStoreFile, keyStorePassword.toCharArray(), keyPassword.toCharArray());
    }
    if (trustStoreFile != null) {
      if (sslContextBuilder == null) {
        sslContextBuilder = SSLContexts.custom();
      }
      sslContextBuilder.loadTrustMaterial(trustStoreFile, trustStorePassword.toCharArray());
    }
    SSLContext sslContext = null;
    if (sslContextBuilder != null) {
      sslContext = sslContextBuilder.build();
    } else {
      sslContext = SSLContexts.createDefault();
      LOG.info("Creating SSLContext using JVM default trust material");
    }

    LOG.info(
        "CyberArk CredentialStore '{}' configured with SSL. Keystore={}, TrustStore={}, Protocols={}, HostNameVerifier={}",
        keyStoreFile,
        trustStoreFile,
        supportedProtocols,
        hostnameVerifier.getClass().getSimpleName()
    );
    sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContext, supportedProtocols, null, hostnameVerifier);
    return sslConnectionSocketFactory;
  }

  @VisibleForTesting
  protected Configuration getConfig() {
    return config;
  }

  @VisibleForTesting
  protected String getUrl() {
    return cyberArkUrl;
  }

  @VisibleForTesting
  protected String getAppId() {
    return appId;
  }

  @VisibleForTesting
  protected String getSeparator() {
    return separator;
  }

  @VisibleForTesting
  protected int getConnectionTimeout() {
    return connectTimeout;
  }

  @VisibleForTesting
  protected String getHttpAuth() {
    return httpAuth;
  }

  @VisibleForTesting
  protected String getHttpAuthUser() {
    return httpAuthUser;
  }

  @VisibleForTesting
  protected String getHttpAuthPassword() {
    return httpAuthPassword;
  }

  @VisibleForTesting
  protected SSLConnectionSocketFactory getSslConnectionSocketFactory() {
    return sslConnectionSocketFactory;
  }

  @VisibleForTesting
  protected PoolingHttpClientConnectionManager getConnectionManager() {
    return connectionManager;
  }

  @VisibleForTesting
  protected RequestConfig getRequestConfig() {
    return requestConfig;
  }

  @VisibleForTesting
  protected CredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  @VisibleForTesting
  public AuthCache getAuthCache() {
    return authCache;
  }

  @VisibleForTesting
  protected HttpClientBuilder getHttpClientBuilder() {
    return httpClientBuilder;
  }

  @VisibleForTesting
  protected CloseableHttpClient getHttpClient() {
    return httpClient;
  }


  @VisibleForTesting
  protected HttpGet createRequest(String safe, String folder, String object, Map<String, String> options)
      throws URISyntaxException {
    URIBuilder uriBuilder =
        new URIBuilder(getUrl()).addParameter(APP_ID_PARAM, getAppId()).addParameter(SAFE_PARAM, safe);
    if (!folder.isEmpty()) {
      uriBuilder.addParameter(FOLDER_PARAM, folder);
    }

    uriBuilder.addParameter(OBJECT_PARAM, object);

    for (Map.Entry<String, String> entry : options.entrySet()) {
      uriBuilder.addParameter(entry.getKey(), entry.getValue());
    }

    uriBuilder.addParameter(FAIL_REQUEST_ON_PASSWORD_CHANGE_PARAM, "true");
    return new HttpGet(uriBuilder.build());
  }

  @VisibleForTesting
  protected String[] decodeName(String name, String separator) {
    String[] splits = name.split(separator);
    if (splits.length < 3) {
      throw new RuntimeException();
    }
    return splits;
  }

  @Override
  public String fetch(String group, String name, Map<String, String> options) throws StageException {
    options = new HashMap<>(options);
    String separator = getSeparator();
    if (options.containsKey(SEPARATOR_CS_PARAM)) {
      separator = options.get(SEPARATOR_CS_PARAM);
      options.remove(SEPARATOR_CS_PARAM);
    }
    if (!options.containsKey(CONNECTION_CS_TIMEOUT_PARAM)) {
      options.put(CONNECTION_TIMEOUT_PARAM, Long.toString(getConnectionTimeout()));
    }
    String[] splits = decodeName(name, separator);
    HttpGet httpGet;
    try {
      httpGet = createRequest(splits[0], splits[1], splits[2], options);
    } catch (Exception ex) {
      throw new StageException(Errors.CYBERARCK_003, config.getId(), getUrl(), ex);
    }
    HttpClientContext httpClientContext = HttpClientContext.create();
    if (getCredentialsProvider() != null) {
      httpClientContext.setCredentialsProvider(getCredentialsProvider());
      httpClientContext.setAuthCache(getAuthCache());
    }
    try (CloseableHttpResponse response = getHttpClient().execute(httpGet, httpClientContext)) {
      if ( response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        Map<String, Object> map = OBJECT_MAPPER.readValue(response.getEntity().getContent(), Map.class);
        String key = "Content";
        if (splits.length > 3) {
          key = splits[3];
        }
        Object value = map.get(key);
        return (value == null) ? null : value.toString();
      } else {
        int code = response.getStatusLine().getStatusCode();
        String message = response.getStatusLine().getReasonPhrase();
        String cyberArkErrorCode = null;
        String cyberArkErrorMessage = null;
        Header header = response.getFirstHeader("Content-Type");
        if (header != null && header.getValue().startsWith("application/json")) {
          try {
            Map<String, Object> map = OBJECT_MAPPER.readValue(response.getEntity().getContent(), Map.class);
            cyberArkErrorCode = (String) map.get("ErrorCode");
            cyberArkErrorMessage = (String) map.get("ErrorMsg");
          } catch (Exception ex) {
            cyberArkErrorCode = "*none*";
            cyberArkErrorMessage = "*none*";
          }
        }
        throw new StageException(Errors.CYBERARCK_004, config.getId(), getUrl(), code, message, cyberArkErrorCode, cyberArkErrorMessage);
      }
    } catch (Exception ex) {
      throw new StageException(Errors.CYBERARCK_003, config.getId(), getUrl(), ex);
    }
  }

  @Override
  public void destroy() {
    if (getHttpClient() != null) {
      try {
        getHttpClient().close();
      } catch (IOException ex) {
        //NOP WARN
      }
      httpClient = null;
    }
  }

}
