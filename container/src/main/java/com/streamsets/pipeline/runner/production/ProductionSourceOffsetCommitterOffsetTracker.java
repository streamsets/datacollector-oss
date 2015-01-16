/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.io.DataStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class ProductionSourceOffsetCommitterOffsetTracker implements SourceOffsetTracker {
  private OffsetCommitter offsetCommitter;
  private String newOffset = ""; // not null to ensure at least one pass

  public ProductionSourceOffsetCommitterOffsetTracker(OffsetCommitter offsetCommitter) {
    this.offsetCommitter = offsetCommitter;
  }

  @Override
  public boolean isFinished() {
    return newOffset == null;
  }

  @Override
  public String getOffset() {
    return newOffset;
  }

  @Override
  public void setOffset(String offset) {
    this.newOffset = offset;
  }

  @Override
  public void commitOffset() {
    if (newOffset != null) {
      try {
        offsetCommitter.commit(newOffset);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

}
