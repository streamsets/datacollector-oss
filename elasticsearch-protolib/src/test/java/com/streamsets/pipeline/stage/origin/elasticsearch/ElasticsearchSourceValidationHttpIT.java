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
package com.streamsets.pipeline.stage.origin.elasticsearch;

import com.streamsets.pipeline.stage.config.elasticsearch.Errors;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups;
import com.streamsets.pipeline.stage.connection.elasticsearch.SecurityMode;
import com.streamsets.pipeline.stage.elasticsearch.common.BaseElasticsearchValidationIT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_09;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_44;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_45;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_46;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_47;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_48;
import static com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups.ELASTIC_SEARCH;
import static com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups.SECURITY;
import static com.streamsets.pipeline.stage.connection.elasticsearch.SecurityMode.AWSSIGV4;
import static com.streamsets.pipeline.stage.connection.elasticsearch.SecurityMode.BASIC;
import static org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400;
import static org.eclipse.jetty.http.HttpStatus.FORBIDDEN_403;
import static org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500;
import static org.eclipse.jetty.http.HttpStatus.NOT_FOUND_404;
import static org.eclipse.jetty.http.HttpStatus.UNAUTHORIZED_401;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@RunWith(Parameterized.class)
public class ElasticsearchSourceValidationHttpIT extends BaseElasticsearchValidationIT {

  @Parameterized.Parameters
  public static Object[][] createTestPathNotFoundFailureParams() {
    return new Object[][] {
        {BAD_REQUEST_400, null, ELASTICSEARCH_44, ELASTIC_SEARCH, "query"},
        {UNAUTHORIZED_401, null, ELASTICSEARCH_47, ELASTIC_SEARCH, "connection.useSecurity"},
        {FORBIDDEN_403, null, ELASTICSEARCH_48, ELASTIC_SEARCH, "connection.useSecurity"},
        {NOT_FOUND_404, null, ELASTICSEARCH_45, ELASTIC_SEARCH, "connection.serverUrl"},
        {INTERNAL_SERVER_ERROR_500, null, ELASTICSEARCH_45, ELASTIC_SEARCH, "connection.serverUrl"},

        {BAD_REQUEST_400, BASIC, ELASTICSEARCH_44, ELASTIC_SEARCH, "query"},
        {UNAUTHORIZED_401, BASIC, ELASTICSEARCH_09, SECURITY, "connection.securityConfig.securityUser"},
        {FORBIDDEN_403, BASIC, ELASTICSEARCH_46, SECURITY, "connection.securityConfig.securityUser"},
        {NOT_FOUND_404, BASIC, ELASTICSEARCH_45, ELASTIC_SEARCH, "connection.serverUrl"},
        {INTERNAL_SERVER_ERROR_500, BASIC, ELASTICSEARCH_45, ELASTIC_SEARCH, "connection.serverUrl"},

        {BAD_REQUEST_400, AWSSIGV4, ELASTICSEARCH_44, ELASTIC_SEARCH, "query"},
        {UNAUTHORIZED_401, AWSSIGV4, ELASTICSEARCH_47, SECURITY, "connection.securityConfig.awsAccessKeyId"},
        {FORBIDDEN_403, AWSSIGV4, ELASTICSEARCH_48, SECURITY, "connection.securityConfig.awsAccessKeyId"},
        {NOT_FOUND_404, AWSSIGV4, ELASTICSEARCH_45, ELASTIC_SEARCH, "connection.serverUrl"},
        {INTERNAL_SERVER_ERROR_500, AWSSIGV4, ELASTICSEARCH_45, ELASTIC_SEARCH, "connection.serverUrl"},
    };
  }

  private final int httpStatusCode;
  private final SecurityMode securityMode;
  private final Errors expectedErrorCode;
  private final ElasticsearchConnectionGroups group;
  private final String conf;

  public ElasticsearchSourceValidationHttpIT(
      final int httpStatusCode,
      final SecurityMode securityMode,
      final Errors expectedErrorCode,
      final ElasticsearchConnectionGroups group,
      final String conf
  ) {
    this.httpStatusCode = httpStatusCode;
    this.securityMode = securityMode;
    this.expectedErrorCode = expectedErrorCode;
    this.group = group;
    this.conf = conf;
  }

  @Test
  public void testValidationHTTPFailure() {
    setupVersionQuery();

    server.when(request().withPath("/_validate/query")).respond(response().withStatusCode(httpStatusCode));

    testHTTPErrorHandling(true, securityMode, group, "conf", conf, expectedErrorCode);
  }
}
