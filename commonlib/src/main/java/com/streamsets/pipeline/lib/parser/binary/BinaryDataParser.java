/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.binary;

import com.google.common.io.ByteStreams;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.io.InputStream;

public class BinaryDataParser implements DataParser {

  private final Stage.Context context;
  private final InputStream is;
  private final String id;
  private final int maxDataLength;
  private boolean parsed;
  private boolean closed;
  private long offset;

  public BinaryDataParser(Stage.Context context, InputStream is, String id, int maxDataLength) {
    this.context = context;
    this.is = is;
    this.id = id;
    this.maxDataLength = maxDataLength;
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    if (closed) {
      throw new IOException("The parser is closed");
    }
    Record record = null;
    if (!parsed) {
      record = context.createRecord(id);
      record.set(Field.create(getDataToParse()));
      parsed = true;
    }
    return record;
  }

  @Override
  public String getOffset() {
    return String.valueOf(offset);
  }

  @Override
  public void close() throws IOException {
    is.close();
    closed = true;
  }

  public byte[] getDataToParse() throws IOException, DataParserException {
    byte[] bytes = ByteStreams.toByteArray(ByteStreams.limit(is, maxDataLength));
    if(maxDataLength == bytes.length) {
      //check if there is more data in the stream than 'maxDataLength'.
      //If yes, the record must be sent to error.
      //Does not make sense truncating binary data as we don't know what it is.
      if(is.read() != -1) {
        throw new DataParserException(Errors.BINARY_PARSER_00, id, maxDataLength);
      }
    }
    offset = bytes.length;
    return bytes;
  }
}
