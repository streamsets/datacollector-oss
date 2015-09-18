/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.pipeline.api.impl.Utils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

@Path("/v1/definitions/helpref")
@Api(value = "definitions")
@DenyAll
public class HelpResource {
  private static final String HELP_REF_NAME = "helpref.properties";
  private static final Properties HELP_REF_PROPERTIES;
  static {
    try {
      URL helpRef = Utils.checkNotNull(HelpResource.class.getClassLoader().getResource(HELP_REF_NAME),
        Utils.formatL("Could not find {}", HELP_REF_NAME));
      try (InputStream is = helpRef.openStream()) {
        try (Reader reader = new InputStreamReader(is)) {
          Properties helpRefs = new Properties();
          helpRefs.load(reader);
          HELP_REF_PROPERTIES = helpRefs;
        }
      }
    } catch (Exception ex) {
      String msg = "Error loading " + HELP_REF_NAME;
      throw new RuntimeException(msg, ex);
    }
  }

  public HelpResource() {  }

  @GET
  @ApiOperation(value = "Returns HELP Reference", response = Map.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getHelpRefs() throws IOException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(HELP_REF_PROPERTIES).build();
  }
}
