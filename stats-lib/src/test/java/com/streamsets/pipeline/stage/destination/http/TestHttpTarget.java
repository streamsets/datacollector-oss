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
package com.streamsets.pipeline.stage.destination.http;

import com.streamsets.testing.NetworkUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.TestProperties;
import org.iq80.snappy.SnappyFramedInputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;
import java.io.IOException;
import java.io.InputStream;

public class TestHttpTarget extends BaseHttpTargetTest {

  @Override
  protected HttpTarget createHttpTarget() {
    return new HttpTarget(getBaseUri() + "send/records", "token", "sdc", "x", "y", 0, true, 5);
  }

  @Override
  protected DeploymentContext configureDeployment() {
    forceSet(TestProperties.CONTAINER_PORT, String.valueOf(NetworkUtils.getRandomPort()));
    ResourceConfig resourceConfig = new ResourceConfig(MockServer.class);
    resourceConfig.register(SnappyReaderInterceptor.class);
    return ServletDeploymentContext.forServlet(
      new ServletContainer(resourceConfig)
    ).build();
  }

  static class SnappyReaderInterceptor implements ReaderInterceptor {

    @Override
    public Object aroundReadFrom(ReaderInterceptorContext context)  throws IOException, WebApplicationException {
      if (context.getHeaders().containsKey("Content-Encoding") &&
        context.getHeaders().get("Content-Encoding").contains("snappy")) {
        InputStream originalInputStream = context.getInputStream();
        context.setInputStream(new SnappyFramedInputStream(originalInputStream, true));
      }
      return context.proceed();
    }
  }
}
