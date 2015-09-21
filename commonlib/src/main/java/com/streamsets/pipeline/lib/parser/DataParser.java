/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.api.Record;

import java.io.Closeable;
import java.io.IOException;

public interface DataParser extends Closeable {

  // returns NULL when reaching EOF
  // throws IOException if closed()
  public Record parse() throws IOException, DataParserException;

  // returns current offset or -1 after reaching OEF
  public String getOffset() throws DataParserException;

  public void setTruncated();

  public void close() throws IOException;

}
