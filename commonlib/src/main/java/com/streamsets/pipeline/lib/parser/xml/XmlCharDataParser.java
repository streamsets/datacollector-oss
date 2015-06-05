/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.xml;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.xml.OverrunStreamingXmlParser;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;

public class XmlCharDataParser implements DataParser {
  private final Stage.Context context;
  private final String readerId;
  private final int maxObjectLen;
  private final OverrunStreamingXmlParser parser;
  private long readerOffset;

  public XmlCharDataParser(Stage.Context context, String readerId, OverrunReader reader, long readerOffset,
                           String recordElement, int maxObjectLen) throws IOException {
    this.context = context;
    this.readerId = readerId;
    this.readerOffset = readerOffset;
    this.maxObjectLen = maxObjectLen;
    try {
      parser = new OverrunStreamingXmlParser(reader, recordElement, readerOffset, maxObjectLen);
    } catch (XMLStreamException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    Record record = null;
    long offset = -1;
    try {
      offset = getOffsetAsLong();
      Field field = parser.read();
      readerOffset = -1;
      if (field != null) {
        record = createRecord(offset, field);
      }
    } catch (XMLStreamException ex) {
      throw new DataParserException(Errors.XML_PARSER_02, readerId, offset, maxObjectLen);
    }
    return record;
  }

  protected Record createRecord(long offset, Field field) throws DataParserException {
    Record record = context.createRecord(readerId + "::" + offset);
    record.set(field);
    return record;
  }

  @Override
  public String getOffset() throws DataParserException {
    return String.valueOf(getOffsetAsLong());

  }

  private long getOffsetAsLong() throws DataParserException {
    try {
      return (readerOffset > -1) ? readerOffset : parser.getReaderPosition();
    } catch (XMLStreamException ex) {
      throw new DataParserException(Errors.XML_PARSER_01, ex.getMessage(), ex);
    }
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

}
