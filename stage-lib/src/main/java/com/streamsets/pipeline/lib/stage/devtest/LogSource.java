/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.devtest;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

@GenerateResourceBundle
@StageDef(version="1.0.1", label="Log files Source")
public class LogSource extends BaseSource {

  @ConfigDef(required = true, type = ConfigDef.Type.STRING, label = "Logs directory", defaultValue = "")
  public String logsDir;

  @ConfigDef(required = true, type = ConfigDef.Type.INTEGER, label = "Rotation frequency (in hr)",
             defaultValue = "24")
  public int rotationFrequency;

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return null;
  }
}
