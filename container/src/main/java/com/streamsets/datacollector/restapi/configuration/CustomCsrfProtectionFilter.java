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
package com.streamsets.datacollector.restapi.configuration;

import com.google.common.collect.ImmutableSet;
import org.glassfish.jersey.server.filter.CsrfProtectionFilter;

import javax.ws.rs.container.ContainerRequestContext;
import java.io.IOException;
import java.util.Set;

public class CustomCsrfProtectionFilter extends CsrfProtectionFilter {

  private static final Set<String> EXCEPTIONS = ImmutableSet.of(
      "/rest/v1/aregistration"
  );

  private static final String POST = "POST";

  @Override
  public void filter(ContainerRequestContext rc) throws IOException {
    if (!(POST.equals(rc.getMethod()) && EXCEPTIONS.contains(rc.getUriInfo().getAbsolutePath().getPath()))) {
      super.filter(rc);
    }
  }

}
