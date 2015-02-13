/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.recordserialization;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;

import java.util.Map;

public interface RecordToString {

  public void setFieldPathToNameMapping(Map<String, String> fieldPathToNameMap);

  public String toString(Record record) throws StageException;

}
