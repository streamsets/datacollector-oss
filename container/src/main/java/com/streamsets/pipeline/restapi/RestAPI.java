/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/ping")
public class RestAPI {

  @GET
  public String ping() {
    return "pong" + System.lineSeparator();
  }

}
