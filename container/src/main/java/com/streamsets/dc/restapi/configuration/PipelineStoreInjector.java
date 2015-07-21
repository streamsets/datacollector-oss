/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.restapi.configuration;

import com.streamsets.pipeline.store.PipelineStoreTask;
import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

public class PipelineStoreInjector implements Factory<PipelineStoreTask> {
  public static final String PIPELINE_STORE = "pipeline-store";
  private PipelineStoreTask store;

  @Inject
  public PipelineStoreInjector(HttpServletRequest request) {
    store = (PipelineStoreTask) request.getServletContext().getAttribute(PIPELINE_STORE);
  }

  @Override
  public PipelineStoreTask provide() {
    return store;
  }

  @Override
  public void dispose(PipelineStoreTask pipelineStore) {
  }

}
