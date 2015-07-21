/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.restapi;

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
      Assert.assertEquals(ex.getCause().getMessage(), "This operation is not supported in SLAVE mode");
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
      Assert.assertEquals(ex.getCause().getMessage(), "This operation is not supported in SLAVE mode");
      exceptionThrown = true;
    } finally {
      Assert.assertTrue(exceptionThrown);
    }
  }

  /*********************************************/
  /*********************************************/


}
