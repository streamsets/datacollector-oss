/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.sdcrecord;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonRecordReader;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;

public class JsonSdcRecordDataParser implements DataParser {
  private final JsonRecordReader recordReader;
  private boolean eof;

  public JsonSdcRecordDataParser(Stage.Context context, OverrunReader reader, long readerOffset, int maxObjectLen)
      throws IOException {
    recordReader = ((ContextExtensions)context).createJsonRecordReader(reader, readerOffset, maxObjectLen);
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    Record record = recordReader.readRecord();
    eof = (record == null);
    return record;
  }

  @Override
  public long getOffset() {
    return (eof) ? -1 : recordReader.getPosition();
  }

  @Override
  public void close() throws IOException {
    recordReader.close();
  }

}
