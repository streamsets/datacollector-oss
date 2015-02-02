/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.xml;

import com.google.common.base.Preconditions;
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

  public OverrunStreamingXmlParser(Reader reader, String recordElement, long initialPosition, int maxObjectLen)
      throws IOException, XMLStreamException {
    super(new OverrunReader(reader, OverrunReader.getDefaultReadLimit(), false), recordElement, initialPosition);
    countingReader = (OverrunReader) getReader();
    countingReader.setEnabled(true);
    this.maxObjectLen = maxObjectLen;
  }

  @Override
  protected void fastForwardLeaseReader() {
    ((OverrunReader) getReader()).resetCount();
  }

  @Override
  protected boolean isInNopMode() throws XMLStreamException {
    return getReaderPosition() > limit;
  }

  @Override
  public Field read() throws IOException, XMLStreamException {
    Field field;
    Preconditions.checkState(!overrun, "The underlying input stream had an overrun, the parser is not usable anymore");
    countingReader.resetCount();
    long initialPosition = getReaderPosition();
    limit = initialPosition + maxObjectLen;
    try {
      field = super.read();
      if (isInNopMode()) {
        throw new XmlObjectLengthException(Utils.format("XML Object at offset '{}' exceeds max length '{}'",
                                                        initialPosition, maxObjectLen), initialPosition);
      }
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
