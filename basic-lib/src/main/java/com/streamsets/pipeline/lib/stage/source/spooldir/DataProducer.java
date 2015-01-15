/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;

import java.io.File;

public interface DataProducer {

  public long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker) throws StageException,
      BadSpoolFileException;

}
