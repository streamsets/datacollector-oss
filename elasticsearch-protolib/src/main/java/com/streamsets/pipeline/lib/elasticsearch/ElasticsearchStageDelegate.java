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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.aws.AwsUtil;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig;
import com.streamsets.pipeline.stage.config.elasticsearch.Errors;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;
import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig.ACCESS_KEY_ID_CONFIG_PATH;
import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig.ENDPOINT_CONFIG_PATH;
import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig.PORT_CONFIG_PATH;
import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig.SERVER_URL_CONFIG_PATH;
import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig.SECURITY_PASSWORD_CONFIG_PATH;
import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig.SECURITY_USER_CONFIG_PATH;
import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG_PATH;
import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig.SSL_TRUSTSTORE_PATH_CONFIG_PATH;
import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig.USE_SECURITY_CONFIG_PATH;
import static com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchSourceConfig.QUERY_CONFIG_PATH;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_09;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_46;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_47;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_48;
import static com.streamsets.pipeline.stage.connection.elasticsearch.SecurityMode.AWSSIGV4;
import static com.streamsets.pipeline.stage.connection.elasticsearch.SecurityMode.BASIC;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

public class ElasticsearchStageDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchStageDelegate.class);
  private static final Pattern URI_PATTERN = Pattern.compile("(https?://)?[^\\r\\n\\v\\f\\t :]+(:(\\d+))?");
  private static final String AWS_SERVICE_NAME = "es";
  private static final JsonParser JSON_PARSER = new JsonParser();
  private static final Gson GSON = new GsonBuilder().setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES).create();
  private static final String VALID_PROPERTY_NAME = "valid";
  private static final String QUERY_PROPERTY_NAME = "query";
  private static final String VALIDATE_QUERY_PATH = "/_validate/query";

  private final Stage.Context context;
  private final ElasticsearchConfig conf;
  private RestClient restClient;
  private Sniffer sniffer;
  private String version = "not-known";
  private int majorVersion = -1;

  public ElasticsearchStageDelegate(Stage.Context context, ElasticsearchConfig conf) {
    this.context = context;
    this.conf = conf;
  }

  public List<Stage.ConfigIssue> init(String prefix, List<Stage.ConfigIssue> issues) {
    if (!conf.connection.port.trim().isEmpty()) {
      try {
        Integer.parseInt(conf.connection.port);
      } catch (final NumberFormatException ex) {
        issues.add(
            getContext().createConfigIssue(
                ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
                prefix + "." + PORT_CONFIG_PATH,
                Errors.ELASTICSEARCH_53,
                conf.connection.port
            )
        );
      }
    }

    List<String> serverURL = conf.connection.getServerURL();

    if (conf.connection.serverUrl.isEmpty()) {
      issues.add(
          context.createConfigIssue(
              ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
              prefix + "." + SERVER_URL_CONFIG_PATH,
              Errors.ELASTICSEARCH_06
          )
      );
    } else {
      for (String uri : serverURL) {
        validateUri(uri, issues, prefix + "." + SERVER_URL_CONFIG_PATH);
      }
    }

    String securityUser = null;
    String securityPassword = null;

    if (conf.connection.useSecurity && BASIC.equals(conf.connection.securityConfig.securityMode)) {
      try {
        securityUser = conf.connection.securityConfig.securityUser.get();
      } catch (StageException e) {
         issues.add(context.createConfigIssue(
             ElasticsearchConnectionGroups.SECURITY.name(),
             prefix + "." + SECURITY_USER_CONFIG_PATH,
             Errors.ELASTICSEARCH_32,
             e.getMessage(),
             e
        ));
      }
      try {
        securityPassword = conf.connection.securityConfig.securityPassword.get();
      } catch (StageException e) {
        issues.add(context.createConfigIssue(
            ElasticsearchConnectionGroups.SECURITY.name(),
            prefix + "." + SECURITY_PASSWORD_CONFIG_PATH,
            Errors.ELASTICSEARCH_38,
            e.getMessage(),
            e
        ));
      }
      if (securityUser == null || securityPassword == null) {
        issues.add(
            context.createConfigIssue(
                ElasticsearchConnectionGroups.SECURITY.name(),
                prefix + "." + SECURITY_USER_CONFIG_PATH,
                Errors.ELASTICSEARCH_40
            )
        );
      } else {
        if (securityUser.isEmpty()) {
          issues.add(
              context.createConfigIssue(
                  ElasticsearchConnectionGroups.SECURITY.name(),
                  prefix + "." + SECURITY_USER_CONFIG_PATH,
                  Errors.ELASTICSEARCH_20
              )
          );
        } else if (!securityUser.contains(":") && securityPassword.isEmpty()) {
          issues.add(
              context.createConfigIssue(
                  ElasticsearchConnectionGroups.SECURITY.name(),
                  prefix + "." + SECURITY_PASSWORD_CONFIG_PATH,
                  Errors.ELASTICSEARCH_39
              )
          );
        }
      }
    }

    if (!issues.isEmpty()) {
      return issues;
    }

    int numHosts = serverURL.size();
    HttpHost[] hosts = new HttpHost[numHosts];
    for (int i = 0; i < numHosts; i++) {
      hosts[i] = HttpHost.create(serverURL.get(i));
    }
    RestClientBuilder restClientBuilder = RestClient.builder(hosts);

    try {
      Response response = null;

      if (conf.connection.useSecurity) {
        SSLContext sslContext = buildSSLContext(prefix, issues, restClientBuilder);
        if (!issues.isEmpty()) {
          return issues;
        }

        switch (conf.connection.securityConfig.securityMode) {
          case BASIC:
            restClient = restClientBuilder.setHttpClientConfigCallback(
                new RestClientBuilder.HttpClientConfigCallback() {
                  @Override
                  public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setSSLContext(sslContext);
                  }
                }
            ).build();
            break;
          case AWSSIGV4:
            AwsRegion awsRegion = conf.connection.securityConfig.awsRegion;
            if (awsRegion == AwsRegion.OTHER) {
              if (conf.connection.securityConfig.endpoint == null || conf.connection.securityConfig.endpoint.isEmpty()) {
                issues.add(context.createConfigIssue(
                    ElasticsearchConnectionGroups.SECURITY.name(),
                    prefix + "." + ENDPOINT_CONFIG_PATH,
                    Errors.ELASTICSEARCH_33
                ));
                return issues;
              }
            }

            HttpRequestInterceptor interceptor = AwsUtil.getAwsSigV4Interceptor(
                AWS_SERVICE_NAME,
                awsRegion,
                conf.connection.securityConfig.endpoint,
                conf.connection.securityConfig.awsAccessKeyId,
                conf.connection.securityConfig.awsSecretAccessKey);
            restClient = restClientBuilder.setHttpClientConfigCallback(hacb ->
                hacb.setSSLContext(sslContext).addInterceptorLast(interceptor)
            ).build();
            break;
        }

        response = restClient.performRequest("GET", "/", getAuthenticationHeader(securityUser, securityPassword));
      } else {
        restClient = restClientBuilder.build();
        response = restClient.performRequest("GET", "/");
      }

      JsonElement version = null;
      String responseBody = readResponseBody(prefix, response, issues);
      JsonElement jsonResponse = parseResponseBody(prefix, responseBody, issues);
      if (jsonResponse != null && jsonResponse.isJsonObject()) {
        version = jsonResponse.getAsJsonObject().get("version");
      }

      if(version != null && version.isJsonObject() && version.getAsJsonObject().get("number") != null) {
        this.version = version.getAsJsonObject().get("number").getAsString();
        this.majorVersion = Integer.parseInt(this.version.split("\\.")[0]);

        LOG.info("ElasticSearch server version {} (major line {})", this.version, this.majorVersion);
      } else {
        LOG.error("Unable to determine ElasticSearch version");
        LOG.debug("Response from server: {}", responseBody);
      }
    } catch (final ResponseException ex) {
      addHTTPResponseError(prefix, SERVER_URL_CONFIG_PATH, "/", ex.getResponse(), issues);
    } catch (final Exception e) {
      issues.add(context.createConfigIssue(
          ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
          prefix + "." + SERVER_URL_CONFIG_PATH,
          Errors.ELASTICSEARCH_43,
          conf.createHostsString(),
          e.getMessage(),
          e
      ));
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

  private boolean addHTTPResponseError(
      final String configPrefix,
      final String failedConfig,
      final String endpoint,
      final Response response,
      final List<Stage.ConfigIssue> issues
  ) {
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode == HttpStatus.SC_BAD_REQUEST) {
      issues.add(context.createConfigIssue(ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
          configPrefix + "." + failedConfig,
          Errors.ELASTICSEARCH_44,
          endpoint
      ));
    } else if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
      addAuthError(configPrefix, endpoint, issues, ELASTICSEARCH_09, ELASTICSEARCH_47);
    } else if (statusCode == HttpStatus.SC_FORBIDDEN) {
      addAuthError(configPrefix, endpoint, issues, ELASTICSEARCH_46, ELASTICSEARCH_48);
    } else {
      issues.add(context.createConfigIssue(ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
          configPrefix + "." + SERVER_URL_CONFIG_PATH,
          Errors.ELASTICSEARCH_45,
          endpoint,
          statusCode,
          response.getStatusLine().getReasonPhrase()
      ));
    }

    return false;
  }

  private void addAuthError(
      final String configPrefix,
      final String endpoint,
      final List<Stage.ConfigIssue> issues,
      final Errors error,
      final Errors anonymousError
  ) {
    if (conf.connection.useSecurity) {
      if (conf.connection.securityConfig.securityMode == BASIC) {
        issues.add(context.createConfigIssue(
            ElasticsearchConnectionGroups.SECURITY.name(),
            configPrefix + "." + SECURITY_USER_CONFIG_PATH,
            error,
            conf.connection.securityConfig.securityUser.get(),
            endpoint
        ));
      } else {
        issues.add(context.createConfigIssue(
            ElasticsearchConnectionGroups.SECURITY.name(),
            configPrefix + "." + ACCESS_KEY_ID_CONFIG_PATH,
            anonymousError,
            endpoint
        ));
      }
    } else {
        issues.add(context.createConfigIssue(
            ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
            configPrefix + "." + USE_SECURITY_CONFIG_PATH,
            anonymousError,
            endpoint
        ));
    }
  }

  /**
   * Sends a request to validate a query. If an error happens during the validation,
   * appropriate issue will be added to the list of config issues.
   *
   * @param prefix - configuration name prefix.
   * @param query - query to validate.
   * @param isIncrementalMode - true if the incremental mode is enabled.
   * @param offsetPlaceholder - pattern of the offset placeholder.
   * @param timeOffset - initial time offset.
   * @param issues - list of config issues.
   */
  public void validateQuery(
      final String prefix,
      final String index,
      final String query,
      final boolean isIncrementalMode,
      final String offsetPlaceholder,
      final String timeOffset,
      final List<Stage.ConfigIssue> issues
  ) {
    if (!issues.isEmpty()) {
      // It means there are prior validation errors that do not even allow to set up a REST client.
      return;
    }

    Header[] headers = conf.connection.useSecurity ? getAuthenticationHeader(
        conf.connection.securityConfig.securityUser.get(),
        conf.connection.securityConfig.securityPassword.get()
    ) : new Header[]{};

    String requestBody = prepareRequestBody(prefix, query, isIncrementalMode, offsetPlaceholder, timeOffset, issues);

    sendRequestAndValidateResponse(prefix, index, headers, requestBody, issues);
  }

  private String prepareRequestBody(
      final String configPrefix,
      final String query,
      final boolean isIncrementalMode,
      final String offsetPlaceholder,
      final String timeOffset,
      final List<Stage.ConfigIssue> issues
  ) {
    String result = null;

    String body = query;
    if (isIncrementalMode) {
      String validatedTimeOffset = timeOffset;
      try {
        Long.parseLong(timeOffset);
      } catch (final NumberFormatException ex) {
        validatedTimeOffset = '"' + timeOffset + '"';
      }
      body = body.replaceAll(offsetPlaceholder, validatedTimeOffset);
    }

    JsonObject json = null;
    try {
      json = JSON_PARSER.parse(body).getAsJsonObject();
    } catch (final JsonSyntaxException | IllegalStateException ex) {
      issues.add(context.createConfigIssue(
          ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(), configPrefix + "." + QUERY_CONFIG_PATH,
          Errors.ELASTICSEARCH_34,
          body,
          ex.getMessage(),
          ex
      ));
    }

    if (json != null) {
      for (final Map.Entry<String, JsonElement> entry : new HashSet<>(json.entrySet())) {
        if (!entry.getKey().equals(QUERY_PROPERTY_NAME)) {
          json.remove(entry.getKey());
        }
      }
      result = GSON.toJson(json);
    }

    return result;
  }

  private void sendRequestAndValidateResponse(
      final String configPrefix,
      final String index,
      final Header[] headers,
      final String requestBody,
      final List<Stage.ConfigIssue> issues
  ) {
    if (requestBody == null) {
      return;
    }
    // issues must be empty, if not - there is a bug in the implementation
    // Should we throw an exception? add an error to the issue list? log the error?

    String endpoint = Optional.ofNullable(index)
        .filter(i -> !i.trim().isEmpty())
        .map(i -> "/" + i)
        .orElse("") + VALIDATE_QUERY_PATH;

    Response response = null;
    try {
      response = restClient.performRequest("POST", endpoint,
          Collections.emptyMap(),
          new StringEntity(requestBody, APPLICATION_JSON),
          headers
      );
    } catch (final ResponseException ex) {
      addHTTPResponseError(configPrefix, QUERY_CONFIG_PATH, endpoint, ex.getResponse(), issues);
    } catch (final IOException ex) {
      issues.add(context.createConfigIssue(
          ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
          configPrefix + "." + SERVER_URL_CONFIG_PATH,
          Errors.ELASTICSEARCH_43,
          conf.createHostsString(),
          ex.getMessage(),
          ex
      ));
    }

    String responseBody = readResponseBody(configPrefix, response, issues);
    validateResponseBody(configPrefix, requestBody, responseBody, issues);
  }

  private String readResponseBody(
      final String configPrefix,
      final Response response,
      final List<Stage.ConfigIssue> issues
  ) {
    if (response == null) {
      return null;
    }

    String responseBody = null;
    try (
      // Converts an input stream into a string.
      // \A means the beginning of the input.
      // hasNext() and next() skip the delimiter at the beginning
      // Since \A doesn't correspond to any character, nothing is skipped.
      // Since there is only one \A  match, next() returns the whole content
      // till the end of the stream.
      Scanner scanner = new Scanner(response.getEntity().getContent()).useDelimiter("\\A")
    ) {
      responseBody = scanner.hasNext() ? scanner.next() : "";
    } catch (final IOException ex) {
      issues.add(context.createConfigIssue(
          ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
          configPrefix + "." + SERVER_URL_CONFIG_PATH,
          Errors.ELASTICSEARCH_42,
          ex
      ));
    }
    return responseBody;
  }

  private void validateResponseBody(
      final String configPrefix,
      final String requestBody,
      final String responseBody,
      final List<Stage.ConfigIssue> issues
  ) {
    if (responseBody == null) {
      return;
    }

    JsonElement json = parseResponseBody(configPrefix, responseBody, issues);
    if (json != null) {
      Boolean valid = extractValidPropertyValue(json);
      if (valid == null) {
        issues.add(context.createConfigIssue(
            ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(), configPrefix + "." + SERVER_URL_CONFIG_PATH,
            Errors.ELASTICSEARCH_49,
            responseBody
        ));
      } else if (!valid) {
        issues.add(context.createConfigIssue(
            ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(), configPrefix + "." + QUERY_CONFIG_PATH,
            Errors.ELASTICSEARCH_41,
            requestBody
        ));
      }
    }
  }

  private JsonElement parseResponseBody(
      final String configPrefix,
      final String responseBody,
      final List<Stage.ConfigIssue> issues
  ) {
    JsonElement json = null;
    try {
      json = JSON_PARSER.parse(responseBody);
    } catch (final JsonSyntaxException ex) {
      issues.add(context.createConfigIssue(
          ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(), configPrefix + "." + SERVER_URL_CONFIG_PATH,
          Errors.ELASTICSEARCH_49,
          responseBody,
          ex
      ));
    }
    return json;
  }

  private Boolean extractValidPropertyValue(final JsonElement json) {
      Boolean valid = null;

    if (json.isJsonObject()) {
      JsonObject jsonObject = json.getAsJsonObject();
      if (jsonObject.has(VALID_PROPERTY_NAME)) {
        JsonElement validElement = jsonObject.get(VALID_PROPERTY_NAME);
        if (validElement.isJsonPrimitive()) {
          JsonPrimitive validPrimitive = validElement.getAsJsonPrimitive();
          if (validPrimitive.isBoolean()) {
            valid = validPrimitive.getAsBoolean();
          }
        }
      }
    }

    return valid;
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

  private SSLContext buildSSLContext(
      final String prefix, List<Stage.ConfigIssue> issues,
      final RestClientBuilder restClientBuilder
  ) throws IOException {
    SSLContext sslContext = null;

    try {
      final String trustStorePath = conf.connection.securityConfig.sslTrustStorePath;
      if (conf.connection.securityConfig.enableSSL) {
        String trustStorePass = null;
        try {
          trustStorePass = conf.connection.securityConfig.sslTrustStorePassword.get();
        } catch (StageException e) {
           issues.add(
              context.createConfigIssue(
                  ElasticsearchConnectionGroups.SECURITY.name(),
                  prefix + "." + SSL_TRUSTSTORE_PASSWORD_CONFIG_PATH,
                  Errors.ELASTICSEARCH_31,
                  e.getMessage(),
                  e
              )
          );
        }

        if (issues.isEmpty() && StringUtils.isEmpty(trustStorePass)) {
          trustStorePass = null;
          issues.add(
              context.createConfigIssue(
                  ElasticsearchConnectionGroups.SECURITY.name(),
                  prefix + "." + SSL_TRUSTSTORE_PASSWORD_CONFIG_PATH,
                  Errors.ELASTICSEARCH_10
              )
          );
        }

        Path path = Paths.get(trustStorePath);
        if (!Files.exists(path)) {
          path = null;
          issues.add(
              context.createConfigIssue(
                  ElasticsearchConnectionGroups.SECURITY.name(),
                  prefix + "." + SSL_TRUSTSTORE_PATH_CONFIG_PATH,
                  Errors.ELASTICSEARCH_11,
                  trustStorePath
              )
          );
        }

        if (path != null && trustStorePass != null) {
          KeyStore keyStore = KeyStore.getInstance("jks");
          try (InputStream is = Files.newInputStream(path)) {
            keyStore.load(is, trustStorePass.toCharArray());
          }
          sslContext = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
        } else {
          sslContext = null;
        }
      } else {
        sslContext = SSLContext.getDefault();
      }
    } catch (IOException | KeyStoreException | NoSuchAlgorithmException | KeyManagementException | CertificateException e) {
      issues.add(
          context.createConfigIssue(
              ElasticsearchConnectionGroups.SECURITY.name(),
              prefix + "." + SSL_TRUSTSTORE_PATH_CONFIG_PATH,
              Errors.ELASTICSEARCH_12,
              Optional.ofNullable(e.getMessage()).orElse("no details provided"),
              e
          )
      );
    }

    return sslContext;
  }

  private void validateUri(String uri, List<Stage.ConfigIssue> issues, String configName) {
    Matcher matcher = URI_PATTERN.matcher(uri);
    if (!matcher.matches()) {
      issues.add(
          getContext().createConfigIssue(
              ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
              configName,
              Errors.ELASTICSEARCH_07,
              uri
          )
      );
    } else if (matcher.group(3) != null) {
      int port = Integer.parseInt(matcher.group(3));
      if (port < 0 || port > 65535) {
        issues.add(
            getContext().createConfigIssue(
                ElasticsearchConnectionGroups.ELASTIC_SEARCH.name(),
                configName,
                Errors.ELASTICSEARCH_08,
                port
            )
        );
      }
    }
  }

  public Header[] getAuthenticationHeader(String securityUser, String securityPassword) {
    if (!conf.connection.useSecurity || conf.connection.securityConfig.securityMode.equals(AWSSIGV4)) {
      return new Header[0];
    }

    // Credentials are in form of "username:password".
    String securityData = (securityUser.contains(":")) ? securityUser:
                          securityUser.concat(":").concat(securityPassword);
    byte[] credentials = securityData.getBytes();
    return Collections.singletonList(new BasicHeader(
        "Authorization",
        "Basic " + Base64.encodeBase64String(credentials)
    )).toArray(new Header[1]);
  }

  private Stage.Context getContext() {
    return context;
  }

  public String getVersion() {
    return version;
  }

  public int getMajorVersion() {
    return majorVersion;
  }
}
