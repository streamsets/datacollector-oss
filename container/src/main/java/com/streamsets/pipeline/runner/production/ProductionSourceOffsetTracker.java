/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.util.JsonFileUtil;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class ProductionSourceOffsetTracker implements SourceOffsetTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionSourceOffsetTracker.class);

  private static final String TEMP_OFFSET_FILE = "offset.json.tmp";
  private static final String OFFSET_FILE = "offset.json";
  private static final String DEFAULT_OFFSET = null;

  private String currentOffset;
  private String newOffset;
  private boolean finished;
  private final String pipelineName;
  private final String rev;
  private final RuntimeInfo runtimeInfo;

  public ProductionSourceOffsetTracker(String pipelineName, String rev, RuntimeInfo runtimeInfo) {
    this.pipelineName = pipelineName;
    this.rev = rev;
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public String getOffset() {
    return getSourceOffset(pipelineName, rev).getOffset();
  }

  @Override
  public void setOffset(String offset) {
    this.newOffset = offset;
  }

  @Override
  public void commitOffset() {
    commitOffset(pipelineName, rev);
  }

  public void commitOffset(String pipelineName, String rev) {
    currentOffset = newOffset;
    finished = (currentOffset == null);
    newOffset = null;
    saveOffset(pipelineName, rev, new SourceOffset(currentOffset));
  }


  public SourceOffset getSourceOffset(String pipelineName, String rev) {
    File pipelineOffsetFile = getPipelineOffsetFile(pipelineName, rev);
    SourceOffset sourceOffset;
    if(pipelineOffsetFile.exists()) {
      //offset file exists, read from it
      try {
        sourceOffset = (SourceOffset) JsonFileUtil.readObjectFromFile(pipelineOffsetFile, SourceOffset.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      sourceOffset = new SourceOffset(DEFAULT_OFFSET);
      saveOffset(pipelineName, rev, sourceOffset);
    }
    return sourceOffset;
  }

  public void resetOffset(String pipelineName, String rev) {
    saveOffset(pipelineName, rev, new SourceOffset(DEFAULT_OFFSET));
  }

  private void saveOffset(String pipelineName, String rev, SourceOffset s) {
    LOG.debug("Saving offset {} for pipeline {}", s.getOffset(), pipelineName);
    try {
      JsonFileUtil.writeObjectToFile(getPipelineOffsetTempFile(pipelineName, rev),
        getPipelineOffsetFile(pipelineName, rev), s);
    } catch (IOException e) {
      LOG.error(Utils.format("Failed to save offset value {}. Reason {}", s.getOffset(), e.getMessage()));
      throw new RuntimeException(e);
    }
  }

  private File getPipelineOffsetFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), OFFSET_FILE);
  }

  private File getPipelineOffsetTempFile(String pipelineName, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev), TEMP_OFFSET_FILE);
  }

}
