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

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchConfig;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchSourceConfig;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchTargetConfig;
import com.streamsets.pipeline.stage.config.elasticsearch.Errors;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnectionGroups;
import com.streamsets.pipeline.stage.connection.elasticsearch.SecurityMode;
import com.streamsets.pipeline.stage.destination.elasticsearch.ElasticSearchDTarget;
import com.streamsets.pipeline.stage.destination.elasticsearch.ElasticsearchTarget;
import com.streamsets.pipeline.stage.origin.elasticsearch.ElasticsearchDSource;
import com.streamsets.pipeline.stage.origin.elasticsearch.ElasticsearchSource;
import org.junit.Rule;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.junit.MockServerRule;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.streamsets.pipeline.stage.lib.aws.AwsRegion.US_WEST_2;
import static java.util.UUID.randomUUID;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public abstract class BaseElasticsearchValidationIT {
  protected static final String HOST_ADDRESS = "127.0.0.1";

  @Rule
  public final MockServerRule mockServerRule = new MockServerRule(this);

  protected MockServerClient server;

  protected <T extends ElasticsearchConfig> T createConf(final boolean source) {
    ElasticsearchConfig conf = null;
    if (source) {
      conf = new ElasticsearchSourceConfig();
      conf.connection.useSecurity = false;
    } else {
      ElasticsearchTargetConfig targetConf = new ElasticsearchTargetConfig();
      targetConf.connection.useSecurity = false;
      targetConf.timeZoneID = "UTC";
      targetConf.timeDriver = "${time:now()}";
      targetConf.indexTemplate = "index";
      targetConf.typeTemplate = "type";
      targetConf.docIdTemplate = "document";
      targetConf.rawAdditionalProperties = "{\n}";
      targetConf.charset = "UTF-8";

      conf = targetConf;
    }
    conf.connection.serverUrl = HOST_ADDRESS;
    conf.connection.port = Optional.ofNullable(mockServerRule.getPort()).map(Object::toString).orElse(null);

    return (T) conf;
  }

  protected void setupVersionQuery() {
    server.when(
        request()
            .withPath("/"))
        .respond(response()
            .withStatusCode(OK_200)
            .withHeader(CONTENT_TYPE, APPLICATION_JSON.getMimeType())
            .withBody("{\"version\": {\"number\": \"7.9.0\"}}"));
  }

  protected String expectedIssue(
      final ElasticsearchConnectionGroups group,
      final String prefix,
      final String conf,
      final ErrorCode errorCode
  ) {
    return "group='" + group + "' config='" + prefix + "." + conf + "' message='" + errorCode.getCode() + " -";
  }

  protected void testErrorHandling(
      final ElasticsearchConfig config,
      final Object... params
  ) {
    if (params.length % 4 != 0) {
      throw new IllegalArgumentException("Invalid additional param count: " + params.length);
    }

    StageRunner<?> runner = (config instanceof ElasticsearchSourceConfig
        ? new PushSourceRunner.Builder(
            ElasticsearchDSource.class,
            new ElasticsearchSource((ElasticsearchSourceConfig) config)
        ).addOutputLane("output")
        : new TargetRunner.Builder(
            ElasticSearchDTarget.class,
            new ElasticsearchTarget((ElasticsearchTargetConfig) config)
        )).build();

    try {
      List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
      Iterator<Stage.ConfigIssue> it = issues.iterator();

      assertThat(issues, hasSize(params.length / 4));
      for (int i = 0; i < params.length / 4; i+= 1) {
        ElasticsearchConnectionGroups group = (ElasticsearchConnectionGroups) params[4 * i + 0];
        String prefix = (String) params[4 * i + 1];
        String confName = (String) params[4 * i + 2];
        Errors errorCode = (Errors) params[4 * i + 3];

        assertThat(it.hasNext(), equalTo(true));

        Stage.ConfigIssue issue = it.next();
        assertThat(issue.toString(), containsString(expectedIssue(group, prefix, confName, errorCode)));
      }

      assertThat(it.hasNext(), equalTo(false));
    } finally {
      runner.runDestroy();
    }
  }

  protected void testHTTPErrorHandling(
      final boolean source,
      final SecurityMode securityMode,
      final ElasticsearchConnectionGroups group,
      final String prefix,
      final String expectedConf,
      final Errors expectedErrorCode
  ) {
    ElasticsearchConfig conf = createConf(source);
    conf.connection.serverUrl = HOST_ADDRESS;
    conf.connection.port = Optional.ofNullable(mockServerRule.getPort()).map(Object::toString).orElse(null);;
    conf.connection.useSecurity = securityMode != null;
    conf.connection.securityConfig.securityMode = securityMode;
    conf.connection.securityConfig.securityUser = () -> randomUUID().toString();
    conf.connection.securityConfig.securityPassword = () -> randomUUID().toString();
    conf.connection.securityConfig.awsAccessKeyId = () -> randomUUID().toString();
    conf.connection.securityConfig.awsSecretAccessKey = () -> randomUUID().toString();
    conf.connection.securityConfig.awsRegion = US_WEST_2;

    testErrorHandling(conf, group, prefix, expectedConf, expectedErrorCode);
  }
}
