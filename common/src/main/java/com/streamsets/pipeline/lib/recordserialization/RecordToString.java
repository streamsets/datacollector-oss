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

  //if set to an EMPTY MAP it means the string representation supports nested structures and the resulting data
  // should mimic the field structure with the exact same names. for impls like CVS, TSV an InvalidArgumentException
  // should be thrown in that case
  public void setFieldPathToNameMapping(Map<String, String> fieldPathToNameMap);

  public String toString(Record record) throws StageException;

}
