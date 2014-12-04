/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.configuration;

import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

public class ProductionPipelineManagerInjector implements Factory<ProductionPipelineManagerTask> {

  public static final String PIPELINE_STATE_MGR = "pipeline-state-mgr";
  private ProductionPipelineManagerTask stateMgr;

  @Inject
  public ProductionPipelineManagerInjector(HttpServletRequest request) {
    stateMgr = (ProductionPipelineManagerTask) request.getServletContext().getAttribute(PIPELINE_STATE_MGR);
  }

  @Override
  public ProductionPipelineManagerTask provide() {
    return stateMgr;
  }

  @Override
  public void dispose(ProductionPipelineManagerTask pipelineStore) {
  }
}
