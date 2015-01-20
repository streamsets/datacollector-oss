/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;

@GenerateResourceBundle
@StageDef(version = "1.0.0",
    label = "HDFS",
    icon = "hdfs.svg",
    description = "Writes records to HDFS files")
public class HdfsTarget extends BaseHdfsTarget {

    @Override
    public void processBatch(Batch batch) throws StageException {

    }
}
