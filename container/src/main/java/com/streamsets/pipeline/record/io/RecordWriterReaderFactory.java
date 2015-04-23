/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.record.io;

import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.RecordWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RecordWriterReaderFactory {

  //10100000
  public static final byte MAGIC_NUMBER_BASE = (byte) 0xa0;

  //10100001
  public static final byte MAGIC_NUMBER_JSON = MAGIC_NUMBER_BASE | (byte) 0x01;


  public static RecordReader createRecordReader(InputStream is, long initialPosition, int maxObjectLen) throws IOException {
    RecordReader reader;
    int read = is.read();
    if (read > -1) {
      byte magicNumber = (byte) read;
      if ((magicNumber & MAGIC_NUMBER_BASE) == MAGIC_NUMBER_BASE) {
        switch (magicNumber) {
          case MAGIC_NUMBER_JSON:
            reader = new JsonRecordReader(is, initialPosition, maxObjectLen);
            break;
          default:
            throw new IOException(String.format("Unsupported magic number '0x%X'", magicNumber));
        }
      } else {
        throw new IOException(String.format("Invalid magic number '0x%X'", magicNumber));
      }
    } else {
      throw new IOException("End of stream");
    }
    return reader;
  }

  public static RecordWriter createRecordWriter(byte magicNumber, OutputStream os) throws IOException {
    RecordWriter writer;
    if ((magicNumber & MAGIC_NUMBER_BASE) == MAGIC_NUMBER_BASE) {
      switch (magicNumber) {
        case MAGIC_NUMBER_JSON:
          os.write(MAGIC_NUMBER_JSON);
          writer = new JsonRecordWriter(os);
          break;
        default:
          throw new IOException(String.format("Unsupported magic number '0x%X'", magicNumber));
      }
    } else {
      throw new IOException(String.format("Invalid magic number '0x%X'", magicNumber));
    }
    return writer;
  }

}
