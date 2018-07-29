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

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.testing.SingleForkNoReuseTest;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.stage.origin.http.PaginationMode.BY_OFFSET;
import static com.streamsets.pipeline.stage.origin.http.PaginationMode.BY_PAGE;
import static com.streamsets.pipeline.stage.origin.http.PaginationMode.LINK_FIELD;
import static com.streamsets.pipeline.stage.origin.http.PaginationMode.LINK_HEADER;
import static com.streamsets.testing.ParametrizedUtils.crossProduct;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(SingleForkNoReuseTest.class)
@RunWith(Parameterized.class)
public class HttpClientSourcePaginationIT extends JerseyTest {
  private static final Logger LOG = LoggerFactory.getLogger(HttpClientSourcePaginationIT.class);

  @Parameterized.Parameters
  public static Collection<Object[]> offsets() {
    return crossProduct(
        new Integer[]{1, 3},
        new Integer[]{2, 4},
        new Object[]{LINK_HEADER, LINK_FIELD, BY_OFFSET, BY_PAGE},
        new Boolean[]{false, true}
    );
  }

  @Parameter
  public int pageNum;

  @Parameter(value = 1)
  public int limit;

  @Parameter(value = 2)
  public PaginationMode mode;

  @Parameter(value = 3)
  public boolean keepAllFields;

  private static List<String> rows = ImmutableList.of(
      "{\"row\": \"1\"}",
      "{\"row\": \"2\"}",
      "{\"row\": \"3\"}",
      "{\"row\": \"4\"}",
      "{\"row\": \"5\"}",
      "{\"row\": \"6\"}",
      "{\"row\": \"7\"}",
      "{\"row\": \"8\"}",
      "{\"row\": \"9\"}",
      "{\"row\": \"10\"}"
  );

  @Path("/paging")
  @Produces("application/json")
  public static class PagingResource {

    @Context
    UriInfo uri;

    @GET
    public Response get(
        @QueryParam("pageNum") int pageNum,
        @QueryParam("limit") int limit,
        @QueryParam("mode") PaginationMode mode
    ) {
      if (pageNum < 1) {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
      Response.ResponseBuilder builder =  Response.ok(getRows(pageNum, limit, mode));

      if (pageNum * limit <= rows.size()) {
        String linkUri = uri.getBaseUri() + "paging?pageNum=" + (pageNum + 1) + "&limit=" + limit;
        builder.link(linkUri, "next");
        LOG.debug("Sending link header: '{}'", linkUri);
      }

      return builder.build();
    }

    private String getRows(int pageNum, int limit, PaginationMode mode) {
      Gson gson = new GsonBuilder().disableHtmlEscaping().create();
      Map<String, Object> envelope = new HashMap<>();
      envelope.put("metadata", "some metadata");
      if (pageNum * limit <= rows.size()) {
        envelope.put("next", uri.getBaseUri() + "paging?pageNum=" + (pageNum + 1) + "&limit=" + limit);
      }

      // 1 is first page
      int startOffset;

      if (mode != BY_OFFSET) {
        startOffset = Math.min(((pageNum - 1) * limit), rows.size());
      } else {
        startOffset = Math.min(pageNum, rows.size());
      }

      List<Map<String, String>> response = new ArrayList<>();

      for (int r = startOffset; r < Math.min(startOffset + limit, rows.size()); r++) {
        Map<String, String> row = new HashMap<>();
        row.put("row", String.valueOf(r+1));
        response.add(row);
      }

      envelope.put("results", response);
      String json = gson.toJson(envelope);
      LOG.info("JSON: {}", json);
      return json;
    }
  }

  @Override
  protected Application configure() {
    forceSet(TestProperties.CONTAINER_PORT, "0");
    return new ResourceConfig(PagingResource.class);
  }

  @Override
  protected DeploymentContext configureDeployment() {
    return ServletDeploymentContext.forServlet(new ServletContainer(new ResourceConfig(PagingResource.class))).build();
  }

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
    return new GrizzlyWebTestContainerFactory();
  }

  @Test
  public void testPaging() throws Exception {
    HttpClientConfigBean conf = getHttpClientConfigBean(pageNum, limit, mode, keepAllFields);

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      int start;
      if (mode != BY_OFFSET) {
        start = (pageNum - 1) * limit;
        assertEquals(10 - start, parsedRecords.size());
      } else {
        start = pageNum;
        assertEquals(10 - start, parsedRecords.size()); // 0 is first record
      }

      int recordNum = start;
      for (Record record : parsedRecords) {
        String rowFieldPath = keepAllFields ? conf.pagination.resultFieldPath + "/row" : "/row";
        assertEquals(++recordNum, record.get(rowFieldPath).getValueAsInteger());
        if (keepAllFields) {
          assertTrue(record.has("/metadata"));
        }
      }
    } finally {
      runner.runDestroy();
    }
  }

  private HttpClientConfigBean getHttpClientConfigBean(
      int start,
      int limit,
      PaginationMode mode,
      boolean keepAllFields
  ) {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.BATCH;
    conf.resourceUrl = getBaseUri() + String.format(
        "paging?pageNum=${startAt}&limit=%d&mode=%s",
        limit,
        mode.name()
    );
    conf.client.readTimeoutMillis = 0;
    conf.client.connectTimeoutMillis = 0;
    conf.client.transferEncoding = RequestEntityProcessing.BUFFERED;
    conf.basic.maxBatchSize = 10;
    conf.basic.maxWaitTime = 10000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    conf.pagination.mode = mode;
    conf.pagination.startAt = start;
    conf.pagination.resultFieldPath = "/results";
    conf.pagination.rateLimit = 0;
    conf.pagination.keepAllFields = keepAllFields;
    conf.pagination.nextPageFieldPath = "/next";
    conf.pagination.stopCondition = "${!record:exists('/next')}";
    return conf;
  }
}
