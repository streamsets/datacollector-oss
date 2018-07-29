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
package com.streamsets.pipeline.lib.parser.net;

import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.ext.io.CountingReader;
import com.streamsets.pipeline.lib.io.CountingInputStream;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.net.syslog.Errors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

public abstract class BaseNetworkMessageDataParser<MT extends MessageToRecord> implements DataParser {

  private final ProtoConfigurableEntity.Context context;
  private final String readerId;
  private final int maxObjectLen;
  private final Long readerOffset;
  private final CountingInputStream inputStream;
  private final CountingReader reader;
  private final Charset charset;

  private boolean decoded;
  protected final List<MT> decodedMessages = new LinkedList<>();
  private ListIterator<MT> decodedMessagesIter;
  private int messageCount;

  protected BaseNetworkMessageDataParser(
      ProtoConfigurableEntity.Context context,
      String readerId,
      InputStream inputStream,
      Long readerOffset,
      int maxObjectLen,
      Charset charset
  ) {
    this(
        context,
        readerId,
        new CountingInputStream(inputStream),
        null,
        readerOffset,
        maxObjectLen,
        charset
    );
  }

  protected BaseNetworkMessageDataParser(
      ProtoConfigurableEntity.Context context,
      String readerId,
      CountingReader reader,
      Long readerOffset,
      int maxObjectLen,
      Charset charset
  ) {
    this(
        context,
        readerId,
        null,
        reader,
        readerOffset,
        maxObjectLen,
        charset
    );
  }

  protected BaseNetworkMessageDataParser(
      ProtoConfigurableEntity.Context context,
      String readerId,
      CountingInputStream inputStream,
      CountingReader reader,
      Long readerOffset,
      int maxObjectLen,
      Charset charset
  ) {
    this.context = context;
    this.readerId = readerId;
    this.maxObjectLen = maxObjectLen;
    this.readerOffset = readerOffset;
    this.inputStream = inputStream;
    this.reader = reader;
    this.charset = charset;
  }

  @Override
  public Record parse() throws IOException, DataParserException {

    if (!decoded) {
      decodeMessages();
    }

    if (decodedMessagesIter.hasNext()) {
      final MT next = decodedMessagesIter.next();
      final Record record = context.createRecord(String.format("%s_%s_%d", readerId, getTypeName(), messageCount++));
      next.populateRecord(record);
      return record;
    } else {
      return null;
    }
  }

  private void decodeMessages() throws IOException, DataParserException {
    byte[] bytes;
    if (inputStream != null) {
      bytes = IOUtils.toByteArray(inputStream);
    } else if (reader != null) {
      bytes = IOUtils.toByteArray(reader, charset);
    } else {
      throw new IllegalStateException(
          "Both inputStream and reader were null in BaseNetworkMessageDataParser#decodeMessages"
      );
    }

    ByteBuf buffer = Unpooled.copiedBuffer(bytes);
    if (maxObjectLen > 0) {
      buffer.capacity( maxObjectLen);
    }
    if (readerOffset != null && readerOffset > 0) {
      buffer.readerIndex(readerOffset.intValue());
    }
    try {
      performDecode(buffer);
      decodedMessagesIter = decodedMessages.listIterator();
      decoded = true;
    } catch (OnRecordErrorException e) {
      throw new DataParserException(Errors.SYSLOG_20, e.getMessage(), e);
    }
  }

  protected abstract String getTypeName();

  protected abstract void performDecode(ByteBuf byteBuf) throws OnRecordErrorException;

  @Override
  public String getOffset() throws DataParserException, IOException {
    return String.valueOf(inputStream.getPos());
  }

  @Override
  public void setTruncated() {
    // nothing to do
  }

  @Override
  public void close() throws IOException {
    // nothing to do
  }
}
