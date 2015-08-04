/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.impl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public interface DataCollector {

  void init() throws Exception;

  void destroy();

  URI getServerURI();

  void startPipeline(String pipelineJson) throws Exception;

  void createPipeline(String pipelineJson) throws Exception;

  void startPipeline() throws Exception;

  void stopPipeline() throws Exception;

  List<URI> getWorkerList() throws URISyntaxException;

  public String storeRules(String name, String tag, String ruleDefinition) throws Exception;
}
