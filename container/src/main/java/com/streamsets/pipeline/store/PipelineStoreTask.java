/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store;

import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RuleDefinitions;
import com.streamsets.pipeline.task.Task;

import java.util.List;

public interface PipelineStoreTask extends Task {
  public static final int SCHEMA_VERSION = 1;

  public PipelineConfiguration create(String name, String description, String user) throws PipelineStoreException;

  public void delete(String name) throws PipelineStoreException;

  public List<PipelineInfo> getPipelines() throws PipelineStoreException;

  public PipelineInfo getInfo(String name) throws PipelineStoreException;

  public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException;

  public PipelineConfiguration save(String name, String user, String tag, String tagDescription,
      PipelineConfiguration pipeline) throws PipelineStoreException;

  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException;

  public boolean hasPipeline(String name);

  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineStoreException;

  public RuleDefinitions storeRules(String pipelineName, String tag, RuleDefinitions ruleDefinitions)
    throws PipelineStoreException;

  public boolean deleteRules(String name) throws PipelineStoreException;

}
