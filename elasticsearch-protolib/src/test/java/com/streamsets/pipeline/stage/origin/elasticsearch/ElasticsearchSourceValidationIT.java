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

import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchSourceConfig;
import com.streamsets.pipeline.stage.elasticsearch.common.BaseElasticsearchValidationIT;
import org.junit.Test;

import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_34;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_41;
import static com.streamsets.pipeline.stage.config.elasticsearch.Errors.ELASTICSEARCH_49;
import static com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups.ELASTIC_SEARCH;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class ElasticsearchSourceValidationIT extends BaseElasticsearchValidationIT {

  @Test
  public void testInvalidQueryJSON() {
    setupVersionQuery();

    ElasticsearchSourceConfig conf = createConf(true);
    conf.query = "{\"}";

    testErrorHandling(conf, ELASTIC_SEARCH, "conf", "query", ELASTICSEARCH_34);
  }

  @Test
  public void testInvalidQuery() {
    setupVersionQuery();

    server.when(
        request()
            .withPath("/_validate/query"))
        .respond(response()
            .withStatusCode(OK_200)
            .withHeader(CONTENT_TYPE, APPLICATION_JSON.getMimeType())
            .withBody("{\"valid\": false}"));

    ElasticsearchSourceConfig conf = createConf(true);
    conf.query = "{}";

    testErrorHandling(conf, ELASTIC_SEARCH, "conf", "query", ELASTICSEARCH_41);
  }

  @Test
  public void testInvalidValidationJSON() {
    setupVersionQuery();

    server.when(
        request()
            .withPath("/_validate/query"))
        .respond(response()
            .withStatusCode(OK_200)
            .withHeader(CONTENT_TYPE, APPLICATION_JSON.getMimeType())
            .withBody("{\""));

    ElasticsearchSourceConfig conf = createConf(true);
    conf.query = "{}";

    testErrorHandling(conf, ELASTIC_SEARCH, "conf", "connection.serverUrl", ELASTICSEARCH_49);
  }

  @Test
  public void testIncompatibleValidationJSON() {
    setupVersionQuery();

    server.when(
        request()
            .withPath("/_validate/query"))
        .respond(response()
            .withStatusCode(OK_200)
            .withHeader(CONTENT_TYPE, APPLICATION_JSON.getMimeType())
            .withBody("{\"valid\": \"true\""));

    ElasticsearchSourceConfig conf = createConf(true);
    conf.query = "{}";

    testErrorHandling(conf, ELASTIC_SEARCH, "conf", "connection.serverUrl", ELASTICSEARCH_49);
  }
}

