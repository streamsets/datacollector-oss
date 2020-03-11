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
package com.streamsets.datacollector.restapi.configuration;


import com.streamsets.datacollector.restapi.rbean.rest.AbstractRestResponse;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * This filter sets the HTTP response code to the response code set in the RestResponse entity.
 */
public class RestResponseFilter implements ContainerResponseFilter {

  public RestResponseFilter() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public void filter(ContainerRequestContext request, ContainerResponseContext response) {
    Object entity = response.getEntity();
    if (entity instanceof AbstractRestResponse) {
      AbstractRestResponse restResponse = (AbstractRestResponse) entity;
      response.setStatus(restResponse.getHttpStatusCode());
    }
  }

}
