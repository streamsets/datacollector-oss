/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.record.io;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.RecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RecordWriterReaderFactory {
  public static final String DATA_COLLECTOR_RECORD_FORMAT = "DATA_COLLECTOR_RECORD_FORMAT";

  private static final Logger LOG = LoggerFactory.getLogger(RecordWriterReaderFactory.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();

  public static RecordReader createRecordReader(InputStream is, long initialPosition, int maxObjectLen)
      throws IOException {
    RecordReader reader;
    int read = is.read();
    if (read > -1) {
      byte magicNumber = (byte) read;
      if ((magicNumber & RecordEncodingConstants.BASE_MAGIC_NUMBER) == RecordEncodingConstants.BASE_MAGIC_NUMBER) {
        RecordEncoding encoding = RecordEncoding.getEncoding(magicNumber);
        switch (encoding) {
          case JSON1:
            reader = new JsonRecordReader(is, initialPosition, maxObjectLen);
            break;
          case KRYO1:
            reader = new KryoRecordReader(is, initialPosition);
            break;
          default:
            throw new RuntimeException("It cannot happen");
        }
        if (IS_TRACE_ENABLED) {
          LOG.trace("Created reader using '{}' encoding", encoding);
        }
      } else {
        throw new IOException(String.format("Invalid magic number '0x%X'", magicNumber));
      }
    } else {
      throw new IOException("End of stream");
    }
    return reader;
  }

  public static RecordWriter createRecordWriter(Stage.Context context, OutputStream os) throws IOException {
    ELVars constants = context.createELVars();
    RecordEncoding encoding = RecordEncoding.getEncoding((String) constants.getConstant(DATA_COLLECTOR_RECORD_FORMAT));
    return createRecordWriter(encoding, os);
  }

  static RecordWriter createRecordWriter(RecordEncoding encoding, OutputStream os) throws IOException {
    RecordWriter writer;
    switch (encoding) {
      case JSON1:
        os.write(RecordEncodingConstants.JSON1_MAGIC_NUMBER);
        writer = new JsonRecordWriter(os);
        break;
      case KRYO1:
        os.write(RecordEncodingConstants.KRYO1_MAGIC_NUMBER);
        writer = new KryoRecordWriter(os);
        break;
      default:
        throw new RuntimeException("It cannot happen");
    }
    LOG.debug("Created writer using '{}' encoding", encoding);
    return writer;
  }

}
