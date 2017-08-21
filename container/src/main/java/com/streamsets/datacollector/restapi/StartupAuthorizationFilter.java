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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

import static com.streamsets.datacollector.restapi.WebServerAgentCondition.canContinue;
import static com.streamsets.datacollector.restapi.WebServerAgentCondition.fail;

@Provider
public class StartupAuthorizationFilter implements ContainerRequestFilter {

  private static final Logger LOG = LoggerFactory.getLogger(StartupAuthorizationFilter.class);

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    if (!canContinue()) {
      LOG.warn("Request for API received, but credentials have not been set yet");
      requestContext.abortWith(fail());
    }
  }
}
