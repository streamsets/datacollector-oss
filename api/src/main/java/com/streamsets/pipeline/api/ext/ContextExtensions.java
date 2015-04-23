/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.ext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface ContextExtensions {

  public RecordReader createRecordReader(InputStream inputStream, long initialPosition, int maxObjectLen)
      throws IOException;

  public  RecordWriter createRecordWriter(OutputStream outputStream) throws IOException;

}
