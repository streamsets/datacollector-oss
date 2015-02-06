/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.Record;

import java.io.IOException;

public interface JsonRecordParser {

  public long getReaderPosition();

  public Record readRecord() throws IOException;

  public void close();

}
