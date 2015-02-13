/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.Record;

import java.io.IOException;

public interface JsonRecordWriter {

  public void write(Record record) throws IOException;

  public void flush() throws IOException;

  public void close();

}
