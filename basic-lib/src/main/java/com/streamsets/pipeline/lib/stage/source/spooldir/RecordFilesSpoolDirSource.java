/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfig;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.FileRawSourcePreviewer;

import java.io.File;

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Bad Records",
    description = "Reads record sent to error by a Data Collector pipeline",
    icon="badrecords.png"
)
@HideConfig({ "filePattern", "initialFileToProcess", "maxSpoolFiles"})
public class RecordFilesSpoolDirSource extends BaseSpoolDirSource {
  private DataProducer dataProducer;

  @Override
  protected void init() throws StageException {
    filePattern = "badrecords-????.json";
    initialFileToProcess = "";
    maxSpoolFiles = 10000;
    super.init();
    dataProducer = new RecordJsonDataProducer(getContext());
  }

  @Override
  public long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker) throws StageException,
      BadSpoolFileException {
    return dataProducer.produce(file, offset, maxBatchSize, batchMaker);
  }

}
