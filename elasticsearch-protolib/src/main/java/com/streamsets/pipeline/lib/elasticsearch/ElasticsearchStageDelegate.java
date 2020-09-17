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
import com.streamsets.pipeline.stage.config.elasticsearch.Groups;
import com.streamsets.pipeline.stage.config.elasticsearch.SecurityConfig;
import com.streamsets.pipeline.stage.config.elasticsearch.SecurityMode;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.entity.StringEntity;
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
import java.io.InputStreamReader;
import java.io.Reader;
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
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

public class ElasticsearchStageDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchStageDelegate.class);
  private static final Pattern URI_PATTERN = Pattern.compile("\\S+:(\\d+)");
  private static final String AWS_SERVICE_NAME = "es";
  private static final JsonParser JSON_PARSER = new JsonParser();
  private static final Gson GSON = new GsonBuilder().setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES).create();
  private static final String VALID_FIELD_NAME = "valid";
  private static final String QUERY_CONFIG_NAME = "query";
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

    String securityPassword = null;
    try {
      securityPassword = conf.securityConfig.securityPassword.get();
    } catch (StageException e) {
      issues.add(context.createConfigIssue(
          Groups.SECURITY.name(),
          SecurityConfig.CONF_PREFIX + "securityPassword",
          Errors.ELASTICSEARCH_38,
          e.toString()
      ));
    }

    if (conf.useSecurity) {
      if (securityUser == null || securityPassword == null) {
        issues.add(
            context.createConfigIssue(
                Groups.SECURITY.name(),
                SecurityConfig.CONF_PREFIX + "securityUser",
                Errors.ELASTICSEARCH_40
            )
        );
      } else {
        if (securityUser.isEmpty()) {
          issues.add(
              context.createConfigIssue(
                  Groups.SECURITY.name(),
                  SecurityConfig.CONF_PREFIX + "securityUser",
                  Errors.ELASTICSEARCH_20
              )
          );
        } else if (!securityUser.contains(":") && securityPassword.isEmpty()) {
          issues.add(
              context.createConfigIssue(
                  Groups.SECURITY.name(),
                  SecurityConfig.CONF_PREFIX + "securityPassword",
                  Errors.ELASTICSEARCH_39
              )
          );
        }
      }
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
      Response response = null;

      if (conf.useSecurity) {
        buildSSLContext(issues, restClientBuilder);

        switch (conf.securityConfig.securityMode) {
          case BASIC:
            restClient = restClientBuilder.build();
            break;
          case AWSSIGV4:
            AwsRegion awsRegion = conf.securityConfig.awsRegion;
            if (awsRegion == AwsRegion.OTHER) {
              if (conf.securityConfig.endpoint == null || conf.securityConfig.endpoint.isEmpty()) {
                issues.add(context.createConfigIssue(Groups.SECURITY.name(), SecurityConfig.CONF_PREFIX + "endpoint", Errors.ELASTICSEARCH_33));
                return issues;
              }
            }

            HttpRequestInterceptor interceptor = AwsUtil.getAwsSigV4Interceptor(
                AWS_SERVICE_NAME,
                awsRegion,
                conf.securityConfig.endpoint,
                conf.securityConfig.awsAccessKeyId,
                conf.securityConfig.awsSecretAccessKey);
            restClient = RestClient.builder(hosts).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)).build();
            break;
        }

        response = restClient.performRequest("GET", "/", getAuthenticationHeader(securityUser, securityPassword));
      } else {
        restClient = restClientBuilder.build();
        response = restClient.performRequest("GET", "/");
      }

      // Finally validate ES version
      JsonObject jsonResponse = parseResponse(response);
      JsonElement version = jsonResponse.get("version");
      if(version != null && version.isJsonObject() && version.getAsJsonObject().get("number") != null) {
        this.version = version.getAsJsonObject().get("number").getAsString();
        this.majorVersion = Integer.parseInt(this.version.split("\\.")[0]);

        LOG.info("ElasticSearch server version {} (major line {})", this.version, this.majorVersion);
      } else {
        LOG.error("Unable to determine ElasticSearch version");
        LOG.debug("Response from server: {}", jsonResponse.toString());
      }
    } catch (Exception e) {
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

  /**
   * Sends a request to valid a query. If an error happens during the validation,
   * appropriate issue will be added to the list of config issues.
   *
   * @param prefix - configuration name prefix.
   * @param query - query to validate.
   * @param isIncrementalMode - true if the incremental mode is enabled.
   * @param offsetPlaceholder - pattern of the offset placeholder.
   * @param timeOffset - initial time offset.
   * @param issues - list of config issues.
   * @return the list of issues.
   */
  public List<Stage.ConfigIssue> validateQuery(
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
      return issues;
    }

    String configName = prefix + "." + QUERY_CONFIG_NAME;

    Header[] headers = conf.useSecurity ? getAuthenticationHeader(
        conf.securityConfig.securityUser.get(),
        conf.securityConfig.securityPassword.get()
    ) : new Header[]{};

    String requestBody = prepareRequestBody(configName, query, isIncrementalMode, offsetPlaceholder, timeOffset, issues);

    sendRequestAndValidateResponse(configName, index, headers, requestBody, issues);

    return issues;
  }

  private String prepareRequestBody(
      final String configName,
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
          Groups.ELASTIC_SEARCH.name(), configName,
          Errors.ELASTICSEARCH_42,
          body,
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
      final String configName,
      final String index,
      final Header[] headers,
      final String requestBody,
      final List<Stage.ConfigIssue> issues
  ) {
    if (requestBody == null) {
      return;
    }

    String endpoint = "/" + index + VALIDATE_QUERY_PATH;

    Response response = null;
    try {
      response = restClient.performRequest("POST", endpoint,
          Collections.emptyMap(),
          new StringEntity(requestBody, APPLICATION_JSON),
          headers
      );
    } catch (final IOException ex) {
      issues.add(context.createConfigIssue(
          Groups.ELASTIC_SEARCH.name(), configName,
          Errors.ELASTICSEARCH_09,
          endpoint,
          ex
      ));
    }

    validateResponse(configName, requestBody, response, issues);
  }

  private void validateResponse(
      final String configName,
      final String requestBody,
      final Response response,
      final List<Stage.ConfigIssue> issues
  ) {
    if (response == null) {
      return;
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
          Groups.ELASTIC_SEARCH.name(), configName,
          Errors.ELASTICSEARCH_43,
          ex
      ));
    }

    validateResponseBody(configName, requestBody, responseBody, issues);
  }

  private void validateResponseBody(
      final String configName,
      final String requestBody,
      final String responseBody,
      final List<Stage.ConfigIssue> issues
  ) {
    if (responseBody == null) {
      return;
    }

    Boolean valid = extractValidPropertyValue(responseBody);
    if (valid == null) {
      issues.add(context.createConfigIssue(
          Groups.ELASTIC_SEARCH.name(), configName,
          Errors.ELASTICSEARCH_42,
          responseBody
      ));
    } else if (!valid) {
      issues.add(context.createConfigIssue(
          Groups.ELASTIC_SEARCH.name(), configName,
          Errors.ELASTICSEARCH_41,
          requestBody
      ));
    }
  }

  private Boolean extractValidPropertyValue(final String responseBody) {
    Boolean valid = null;
    JsonElement json = JSON_PARSER.parse(responseBody);
    if (json.isJsonObject()) {
      JsonObject jsonObject = json.getAsJsonObject();
      if (jsonObject.has(VALID_FIELD_NAME)) {
        JsonElement validElement = jsonObject.get(VALID_FIELD_NAME);
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

  public JsonObject parseResponse(Response response) throws IOException {
    Reader reader = new InputStreamReader(response.getEntity().getContent());
    return JSON_PARSER.parse(reader).getAsJsonObject();
  }

  public JsonObject performRequestAndParseAnswer(
      String method,
      String endpoint,
      Map<String, String> params,
      HttpEntity entity,
      Header... headers
  ) throws IOException {
    Response response = performRequest(method, endpoint, params, entity, headers);
    return parseResponse(response);
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

  public Header[] getAuthenticationHeader(String securityUser, String securityPassword) {
    if (!conf.useSecurity || conf.securityConfig.securityMode.equals(SecurityMode.AWSSIGV4)) {
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
