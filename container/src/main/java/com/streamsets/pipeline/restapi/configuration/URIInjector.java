/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.configuration;

import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.URISyntaxException;

public class URIInjector implements Factory<URI> {
  private URI uri;

  @Inject
  public URIInjector(HttpServletRequest request) {
    try {
      uri = new URI(request.getRequestURI());
    } catch (URISyntaxException ex) {
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
