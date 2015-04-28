/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.configuration;

import com.streamsets.pipeline.prodmanager.PipelineManager;
import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

public class ProductionPipelineManagerInjector implements Factory<PipelineManager> {

  public static final String PIPELINE_MANAGER_MGR = "pipeline-manager";

  private PipelineManager pipelineManager;

  @Inject
  public ProductionPipelineManagerInjector(HttpServletRequest request) {
    pipelineManager = (PipelineManager) request.getServletContext().getAttribute(PIPELINE_MANAGER_MGR);
  }

  @Override
  public PipelineManager provide() {
    return pipelineManager;
  }

  @Override
  public void dispose(PipelineManager manager) {
  }
}
