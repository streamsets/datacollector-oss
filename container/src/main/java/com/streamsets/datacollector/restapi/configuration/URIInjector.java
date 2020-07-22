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

import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;

public class URIInjector implements Factory<URI> {
  private URI uri;

  @Inject
  public URIInjector(HttpServletRequest request) {
    try {
      uri = new URI(URLEncoder.encode(request.getRequestURI(),"UTF-8"));
    } catch (URISyntaxException | UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public URI provide() {
    return uri;
  }

  @Override
  public void dispose(URI uri) {
  }

}
