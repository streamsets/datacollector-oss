/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.xml;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.ext.io.ObjectLengthException;
import com.streamsets.pipeline.api.ext.io.OverrunException;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;

public class OverrunStreamingXmlParser  extends StreamingXmlParser {

  private final OverrunReader countingReader;
  private final int maxObjectLen;
  private long limit;
  private boolean overrun;
  private long initialPosition;

  public OverrunStreamingXmlParser(Reader reader, String recordElement, long initialPosition, int maxObjectLen)
      throws IOException, XMLStreamException {
    this(
        new OverrunReader(
            reader,
            OverrunReader.getDefaultReadLimit(),
            false,
            false
        ),
        recordElement,
        null,
        initialPosition,
        maxObjectLen,
        true
    );
    this.initialPosition = initialPosition;
  }

  public OverrunStreamingXmlParser(OverrunReader reader, String recordElement, Map<String, String> namespaces,
      long initialPosition, int maxObjectLen, boolean useFieldAttributesInsteadOfFields)
      throws IOException, XMLStreamException {
    super(reader, recordElement, namespaces, initialPosition, useFieldAttributesInsteadOfFields);
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
    return (maxObjectLen > -1) && getReaderPosition() > limit;
  }

  @Override
  public Field read() throws IOException, XMLStreamException {
    Field field;
    Utils.checkState(!overrun, "The underlying input stream had an overrun, the parser is not usable anymore");
    countingReader.resetCount();
    limit = getReaderPosition() + maxObjectLen;
    try {
      field = super.read();
      throwIfOverMaxObjectLength();
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

  @Override
  protected void throwIfOverMaxObjectLength() throws XMLStreamException, ObjectLengthException {
    if (isOverMaxObjectLength()) {
      throw new ObjectLengthException(
          Utils.format("XML Object at offset '{}' exceeds max length '{}'; current position '{}'",
              initialPosition,
              maxObjectLen,
              getReaderPosition()
          ),
          getReaderPosition()
      );
    }
  }

}
