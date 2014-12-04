/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.configuration;

import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

public class StageLibraryInjector implements Factory<StageLibraryTask> {
  public static final String STAGE_LIBRARY = "stage-library";

  private StageLibraryTask library;

  @Inject
  public StageLibraryInjector(HttpServletRequest request) {
    library = (StageLibraryTask) request.getServletContext().getAttribute(STAGE_LIBRARY);
  }

  @Override
  public StageLibraryTask provide() {
    return library;
  }

  @Override
  public void dispose(StageLibraryTask library) {
  }

}
