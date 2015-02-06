/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;

import java.util.List;

public interface RecordCreator {

  public List<Record> createRecords(MessageAndOffset message, int currentRecordCount) throws StageException;

}
