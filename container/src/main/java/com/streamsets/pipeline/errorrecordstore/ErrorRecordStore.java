/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.errorrecordstore;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface ErrorRecordStore {

  public void storeErrorRecords(String pipelineName, String rev, Map<String, List<Record>> errorRecords);

  public void storeErrorMessages(String pipelineName, String rev, Map<String, List<ErrorMessage>> errorMessages);

  public void deleteErrors(String pipelineName, String rev);

  public InputStream getErrors(String pipelineName, String rev);

  public void register(String pipelineName, String rev);
}
