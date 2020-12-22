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
package com.streamsets.pipeline.stage.elasticsearch.common;

import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchSourceConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.UUID;

import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_10;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_11;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_12;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_43;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_49;
import static com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups.ELASTIC_SEARCH;
import static com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups.SECURITY;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.socket.PortFactory.findFreePort;

@RunWith(Parameterized.class)
public class ElasticsearchValidationIT extends BaseElasticsearchValidationIT {

  @Parameterized.Parameters
  public static Object[][] createParams() {
    return new Object[][] {
        {false, "elasticSearchConfig"},
        {true, "conf"}
    };
  }

  private final boolean source;
  private final String prefix;

  public ElasticsearchValidationIT(final boolean source, final String prefix) {
    this.source = source;
    this.prefix = prefix;
  }

  @Test
  public void testConnectionFailure() {
    ElasticsearchConfig conf = createConf(source);
    conf.connection.serverUrl = HOST_ADDRESS;
    conf.connection.port = "" + findFreePort();

    testErrorHandling(conf, ELASTIC_SEARCH, prefix, "connection.serverUrl", ELASTICSEARCH_43);
  }

  @Test
  public void testInvalidVersionJSON() {
    server.when(
        request()
            .withPath("/"))
        .respond(response()
            .withStatusCode(OK_200)
            .withHeader(CONTENT_TYPE, APPLICATION_JSON.getMimeType())
            .withBody("{\""));

    ElasticsearchConfig conf = createConf(source);

    if (source) {
      ((ElasticsearchSourceConfig) conf).query = "{}";
    }

    testErrorHandling(conf, ELASTIC_SEARCH, prefix, "connection.serverUrl", ELASTICSEARCH_49);
  }

  @Test
  public void testInvalidTruststoreCredentials() {
    ElasticsearchConfig conf = createConf(source);
    conf.connection.useSecurity = true;
    conf.connection.securityConfig.enableSSL = true;
    conf.connection.securityConfig.securityUser = () -> UUID.randomUUID().toString();
    conf.connection.securityConfig.securityPassword = () -> UUID.randomUUID().toString();
    conf.connection.securityConfig.sslTrustStorePath = UUID.randomUUID().toString();

    testErrorHandling(
        conf,
        SECURITY, prefix, "connection.securityConfig.sslTrustStorePassword", ELASTICSEARCH_10,
        SECURITY, prefix, "connection.securityConfig.sslTrustStorePath", ELASTICSEARCH_11
    );
  }

  @Test
  public void testInvalidTruststore() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    assertThat(dir.mkdirs(), equalTo(true));

    String truststorePath = new File(dir.getAbsolutePath(), "invalid.jks").getAbsolutePath();
    try (OutputStream os = new FileOutputStream(truststorePath)) {}

    ElasticsearchConfig conf = createConf(source);
    conf.connection.useSecurity = true;
    conf.connection.securityConfig.enableSSL = true;
    conf.connection.securityConfig.securityUser = () -> UUID.randomUUID().toString();
    conf.connection.securityConfig.securityPassword = () -> UUID.randomUUID().toString();
    conf.connection.securityConfig.sslTrustStorePassword = () -> UUID.randomUUID().toString();
    conf.connection.securityConfig.sslTrustStorePath = truststorePath;

    testErrorHandling(conf, SECURITY, prefix, "connection.securityConfig.sslTrustStorePath", ELASTICSEARCH_12);
  }
}

