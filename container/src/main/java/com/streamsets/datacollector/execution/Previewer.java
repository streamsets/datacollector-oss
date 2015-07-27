/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.PipelineException;

import javax.ws.rs.core.MultivaluedMap;

import java.util.List;

public interface Previewer {

  // a Previewer lives for duration of a preview pipeline operation (validateConfig or start)

  // we should have a implementation that has a synchronous start() (by the time it finishes the status is final)

  // we should have a proxy implementation that uses the synchronous one and calls start() within a separate thread
  // making it the start() asynchronous. The manager should cache this one.

  // Implementations receive a PreviewerListener at <init> time, the listener is owned by the Manager

  public String getId();

  public String getName();

  public String getRev();

  public void validateConfigs(long timeoutMillis) throws PipelineException;

  public RawPreview getRawSource(int maxLength, MultivaluedMap<String, String> previewParams)
    throws PipelineRuntimeException, PipelineStoreException;

  public void start(int batches, int batchSize, boolean skipTargets, String stopStage, List<StageOutput> stagesOverride,
                    long timeoutMillis) throws PipelineException;

  public void stop();

  // in the case of the synchronous one the only acceptable value is -1 (wait until it finishes)
  // in the case of the asynchronous one acceptable values are 0 (dispatch and wait) and greater (dispatch and block for millis)
  public boolean waitForCompletion(long timeoutMillis) throws PipelineException;

  public PreviewStatus getStatus();

  public PreviewOutput getOutput();

}
