/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.ext;

import com.streamsets.pipeline.api.Record;

import java.io.Closeable;
import java.io.IOException;

public interface JsonRecordReader extends Closeable {

  public long getPosition();

  public Record readRecord() throws IOException;

  public void close() throws IOException;

}
