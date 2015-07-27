/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.util.List;

public interface ErrorStore {

  public void saveErrors(String name, String rev, List<ErrorMessage> errorMessages, List<Record> errorRecords);

  public List<Record> getErrorRecords(String name, String rev, String stage, int max);

  public List<ErrorMessage> getErrorMessages(String name, String rev, String stage, int max);

}
