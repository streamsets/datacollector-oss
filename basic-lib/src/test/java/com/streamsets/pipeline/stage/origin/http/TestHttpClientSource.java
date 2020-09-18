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
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.http.AbstractHttpStageTest;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import com.streamsets.pipeline.sdk.SourceRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
@PrepareForTest({HttpClientSource.class, JerseyClientUtil.class})
public class TestHttpClientSource extends AbstractHttpStageTest {

  public HttpClientConfigBean getConf(String url) {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = url;
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    return conf;
  }

  @Override
  public List<Stage.ConfigIssue> runStageValidation(String uri) throws Exception {
    HttpClientConfigBean conf = getConf("http://localhost:10000");
    conf.client.useProxy = true;
    conf.client.proxy.uri = uri;

    HttpClientSource origin = PowerMockito.spy(new HttpClientSource(conf));

    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    return runner.runValidateConfigs();
  }

  @Test
  public void nextPageLinkStoredToOffset() throws StageException {
    final HttpClientConfigBean conf = getConf("http://api.example.com/getItems");
    conf.pagination.mode = PaginationMode.LINK_FIELD;
    conf.pagination.stopCondition = "${false}";
    conf.pagination.nextPageURLPrefix = "http://api.example.com";
    conf.pagination.nextPageFieldPath = "/nextPageLink";
    conf.pagination.resultFieldPath = "/records";
    conf.basic.maxWaitTime = 30000;

    final HttpClientSource source = PowerMockito.spy(new HttpClientSource(conf));
    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, source)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    runner.runInit();

    final String nextPage1 = "http://api.example.com/getItems?cursorId=12345";
    final String recordJson1 = String.format(
        "{\"records\": [{\"field1\": 1, \"field2\": 2}, {\"field1\": 3, \"field2\": 4}], \"nextPageLink\": \"%s\"}",
        nextPage1
    );

    final String nextPage2 = "http://api.example.com/getItems?cursorId=12345&startFrom=2";
    final String recordJson2 = String.format(
        "{\"records\": [{\"field1\": 5, \"field2\": 6}, {\"field1\": 7, \"field2\": 8}], \"nextPageLink\": \"%s\"}",
        nextPage2
    );

    final String nextPage3 = "/getItems?cursorId=12345&startFrom=3";
    final String recordJson3 = String.format(
        "{\"records\": [{\"field1\": 9, \"field2\": 10}, {\"field1\": 11, \"field2\": 12}], \"nextPageLink\": \"%s\"}",
        nextPage3
    );

    final String[] records = {recordJson1, recordJson2, recordJson3};

    final Response mockResponse1 = PowerMockito.mock(Response.class);
    PowerMockito.doAnswer(new Answer<InputStream>() {
      private int invocationCount;
      @Override
      public InputStream answer(InvocationOnMock invocation) throws Throwable {
        if (invocationCount > 2) {
          throw new IllegalStateException("Should not be called more than 3 times");
        }
        final String json = records[invocationCount++];

        return new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
      }
    }).when(mockResponse1).readEntity(InputStream.class);

    PowerMockito.doAnswer(invocation -> 0).when(source).getCurrentPage();

    source.setResolvedUrl(source.resolveInitialUrl(null));
    // initial request; target is the configured resource URL
    source.setResponse(mockResponse1);
    // the first response should contain the next page URL
    final String offsetStr1 = source.parseResponse(
        Clock.systemUTC().millis(),
        10,
        PowerMockito.mock(BatchMaker.class)
    );
    source.resolveNextPageUrl(offsetStr1);
    final HttpSourceOffset offset1 = HttpSourceOffset.fromString(offsetStr1);
    // and the committed offset should be the starting URL
    assertThat(offset1.getUrl(), equalTo(conf.resourceUrl));
    // but the next page URL should be the value parsed from the first record
    final String nextTarget = source.getResolvedUrl();
    assertThat(nextTarget, equalTo(nextPage1));

    // set up the mock again (since response was set to null by cleanup of last request handling)
    source.setResponse(mockResponse1);
    // the next response should contain a new next page
    final String offsetStr2 = source.parseResponse(
        Clock.systemUTC().millis(),
        10,
        PowerMockito.mock(BatchMaker.class)
    );
    source.resolveNextPageUrl(offsetStr2);
    final HttpSourceOffset offset2 = HttpSourceOffset.fromString(offsetStr2);
    // the committed offset should now be the previous starting URL
    assertThat(offset2.getUrl(), equalTo(nextPage1));
    final String nextTarget2 = source.getResolvedUrl();
    // and the next page URL should be the value parsed from the second record
    assertThat(nextTarget2, equalTo(nextPage2));

    // set up the mock again (since response was set to null by cleanup of last request handling)
    source.setResponse(mockResponse1);
    // the next response should contain a new next page
    final String offsetStr3 = source.parseResponse(
        Clock.systemUTC().millis(),
        10,
        PowerMockito.mock(BatchMaker.class)
    );
    source.resolveNextPageUrl(offsetStr3);
    final HttpSourceOffset offset3 = HttpSourceOffset.fromString(offsetStr3);
    // the committed offset should now be the previous starting URL
    assertThat(offset3.getUrl(), equalTo(nextPage2));
    final String nextTarget3 = source.getResolvedUrl();
    // the prefix should be appended to the relative URL in this case
    assertThat(nextTarget3, equalTo(conf.pagination.nextPageURLPrefix + nextPage3));
  }

}
