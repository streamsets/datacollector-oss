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
package com.streamsets.datacollector.restapi;

import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.ws.rs.core.Response;

import java.io.IOException;

// TODO - fix after refactoring
@Ignore
public class TestPipelineManagerResourceForSlaveMode extends JerseyTest {

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "2.0";
  private static final String DEFAULT_PIPELINE_REV = "0";


  @Test
  public void testStartPipelineAPI() throws IllegalStateException {
    boolean exceptionThrown = false;
    try {
      Response r = target("/v1/pipeline/start").queryParam("name", PIPELINE_NAME).queryParam("rev", PIPELINE_REV)
        .request().post(null);
    } catch (Exception ex) {
      Assert.assertEquals(ex.getCause().toString(), "This operation is not supported in SLAVE mode");
      exceptionThrown = true;
    } finally {
      Assert.assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testStopPipelineAPI() {
    boolean exceptionThrown = false;
    try {
      Response r = target("/v1/pipeline/stop").queryParam("rev", PIPELINE_REV)
        .request().post(null);
    } catch (Exception ex) {
      Assert.assertEquals(ex.getCause().getMessage(), "This operation is not supported in SLAVE mode");
      exceptionThrown = true;
    } finally {
      Assert.assertTrue(exceptionThrown);
    }

  }

  @Test
  public void testResetOffset() throws IOException {
    boolean exceptionThrown = false;
    try {
      Response r = target("/v1/pipeline/resetOffset/myPipeline").request().post(null);
    } catch (Exception ex) {
      Assert.assertEquals(ex.getCause().toString(), "This operation is not supported in SLAVE mode");
      exceptionThrown = true;
    } finally {
      Assert.assertTrue(exceptionThrown);
    }
  }

  /*********************************************/
  /*********************************************/


}
