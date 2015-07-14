/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator;

import com.streamsets.pipeline.api.Record;

import java.io.Closeable;
import java.io.IOException;

public interface DataGenerator extends Closeable {

  public void write(Record record)  throws IOException, DataGeneratorException;

  public void flush() throws IOException;

  public void close() throws IOException;

}
