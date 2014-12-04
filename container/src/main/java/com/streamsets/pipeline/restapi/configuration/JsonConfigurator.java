/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.json.ObjectMapperFactory;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class JsonConfigurator implements ContextResolver<ObjectMapper> {
  private ObjectMapper objectMapper;

  public JsonConfigurator() throws Exception {
    objectMapper = ObjectMapperFactory.get();
  }

  @Override
  public ObjectMapper getContext(Class<?> objectType) {
    return objectMapper;
  }

}
