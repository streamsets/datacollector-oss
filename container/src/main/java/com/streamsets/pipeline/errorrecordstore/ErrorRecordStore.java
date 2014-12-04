/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.errorrecordstore;

import com.streamsets.pipeline.api.Record;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface ErrorRecordStore {

  void storeErrorRecords(String pipelineName, String rev, Map<String, List<Record>> errorRecords);

  void deleteErrorRecords(String pipelineName, String rev, String stageInstanceName);

  InputStream getErrorRecords(String pipelineName, String rev, String stageInstanceName);
}
