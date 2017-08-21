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
package com.streamsets.pipeline.stage.util.http;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HttpStageTestUtil {

  public static final Map<String, String> CONTENT_TYPE_TO_BODY = new HashMap<>();

  static {
    CONTENT_TYPE_TO_BODY.put("application/json", "{\"key\": \"value\"}");
    CONTENT_TYPE_TO_BODY.put("application/xml", "<root><record>value</record></root>");
    CONTENT_TYPE_TO_BODY.put("text/plain", "value");
    CONTENT_TYPE_TO_BODY.put("application/custom", "custom_value");
    CONTENT_TYPE_TO_BODY.put("", "default_value");
  }

  @NotNull
  public static String randomizeCapitalization(Random random, String input) {
    StringBuilder header = new StringBuilder();
    for (int i=0; i<input.length(); i++) {
      String nextChar = input.substring(i, i+1);
      if (random.nextBoolean()) {
        header.append(nextChar.toUpperCase());
      } else {
        header.append(nextChar.toLowerCase());
      }
    }
    return header.toString();
  }

  @Path("/test/postCustomType")
  @Consumes("*/*")
  public static class TestPostCustomType {
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes("*/*")
    public Response echoContentTypeAndBody(
        @HeaderParam(HttpStageUtil.CONTENT_TYPE_HEADER) String contentType,
        String body
    ) {
      return Response.ok(
          "{\""+ HttpStageUtil.CONTENT_TYPE_HEADER+"\": \"" +
          contentType +
          "\", \"Content\": \"" +
          StringEscapeUtils.escapeJson(body) +
          "\"}"
      ).build();
    }
  }
}
