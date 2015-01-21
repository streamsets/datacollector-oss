/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.recordserialization;

import com.streamsets.pipeline.api.Record;

import java.util.Map;

public class XmlRecordToString implements RecordToString {

  @Override
  public void setFieldPathToNameMapping(Map<String, String> fieldPathToNameMap) {
    //Throwing an exception here can cause trouble.
    //For example if the user selects CSV and enters a big list of field path to column name mapping
    //and then switches to JSON, the UI does not clear the previous information. What if the user changed from CSV
    //to JSON by mistake? We should not expect everything to be reentered.

    //In this case it is good to ignore options that are not relevant. UI hides non relevant option anyways.

  }

  @Override
  public String toString(Record record) {
    return "<record/>";
  }

}
