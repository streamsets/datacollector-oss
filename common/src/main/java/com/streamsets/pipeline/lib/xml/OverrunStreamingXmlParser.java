/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.xml;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.io.OverrunReader;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.Reader;

public class OverrunStreamingXmlParser  extends StreamingXmlParser {

  private final OverrunReader countingReader;
  private final int maxObjectLen;
  private long limit;
  private boolean overrun;
  private long initialPosition;

  public OverrunStreamingXmlParser(Reader reader, String recordElement, long initialPosition, int maxObjectLen)
      throws IOException, XMLStreamException {
    this(new OverrunReader(reader, OverrunReader.getDefaultReadLimit(), false), recordElement, initialPosition,
         maxObjectLen);
    this.initialPosition = initialPosition;
  }

  public OverrunStreamingXmlParser(OverrunReader reader, String recordElement, long initialPosition, int maxObjectLen)
      throws IOException, XMLStreamException {
    super(reader, recordElement, initialPosition);
    countingReader = (OverrunReader) getReader();
    countingReader.setEnabled(true);
    this.maxObjectLen = maxObjectLen;
    this.initialPosition = initialPosition;
  }

  @Override
  protected void fastForwardLeaseReader() {
    ((OverrunReader) getReader()).resetCount();
  }

  @Override
  protected boolean isOverMaxObjectLength() throws XMLStreamException {
    return getReaderPosition() > limit;
  }

  @Override
  public Field read() throws IOException, XMLStreamException {
    Field field;
    Utils.checkState(!overrun, "The underlying input stream had an overrun, the parser is not usable anymore");
    countingReader.resetCount();
    limit = getReaderPosition() + maxObjectLen;
    try {
      field = super.read();
      if (isOverMaxObjectLength()) {
        throw new XmlObjectLengthException(Utils.format("XML Object at offset '{}' exceeds max length '{}'",
                                                        initialPosition, maxObjectLen), initialPosition);
      }
      initialPosition = getReaderPosition();
    } catch (XMLStreamException ex) {
      if (ex.getNestedException() != null && ex.getNestedException() instanceof OverrunException) {
        overrun = true;
        throw (OverrunException) ex.getNestedException();
      }
      throw ex;
    }
    return field;
  }

  public static class XmlObjectLengthException extends IOException {
    private long readerOffset;

    public XmlObjectLengthException(String message, long offset) {
      super(message);
      readerOffset = offset;
    }

    public long getXmlOffset() {
      return readerOffset;
    }

  }

}
