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
package com.streamsets.pipeline.lib.parser.udp.separated;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.net.raw.RawDataMode;
import com.streamsets.pipeline.lib.parser.udp.AbstractParser;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class SeparatedDataParser extends AbstractParser {

  public static final String SENDER_HEADER_ATTR_NAME = "UDP_Sender";
  public static final String RECIPIENT_HEADER_ATTR_NAME = "UDP_Recipient";

  private final RawDataMode rawDataMode;
  private final Charset charset;
  private final String outputFieldPath;
  private final byte[] separatorBytes;

  private long recordId = 0L;

  public SeparatedDataParser(
      Stage.Context context,
      RawDataMode rawDataMode,
      Charset charset,
      String outputFieldPath,
      MultipleValuesBehavior multipleValuesBehavior,
      byte[] separatorBytes
  ) {
    super(context);
    this.rawDataMode = rawDataMode;
    this.charset = charset;
    this.outputFieldPath = outputFieldPath;
    this.separatorBytes = separatorBytes;
  }

  @Override
  public List<Record> parse(
      ByteBuf buf, InetSocketAddress recipient, InetSocketAddress sender
  ) throws OnRecordErrorException {

    List<Record> records = new LinkedList<>();

    final byte[] data = new byte[buf.readableBytes()];
    buf.getBytes(0, data);

    for (byte[] chunk : split(data, separatorBytes)) {
      final Record record = context.createRecord(String.format("SeparatedDataParser_%d", recordId++));

      record.set(Field.create(Collections.emptyMap()));
      switch (rawDataMode) {
        case CHARACTER:
          record.set(outputFieldPath, Field.create(new String(chunk, charset)));
          break;
        case BINARY:
          record.set(outputFieldPath, Field.create(chunk));
          break;
        default:
          throw new IllegalStateException(String.format("Unrecognized rawDataMode: %s", rawDataMode.name()));
      }

      if (sender != null) {
        record.getHeader().setAttribute(SENDER_HEADER_ATTR_NAME, sender.toString());
      }
      if (recipient != null) {
        record.getHeader().setAttribute(RECIPIENT_HEADER_ATTR_NAME, recipient.toString());
      }
      records.add(record);
    }

    return records;
  }

  /**
   *
   * Adapted from <a href="https://stackoverflow.com/a/44468124/375670">this StackOverflow answer</a>
   * @param array
   * @param delimiter
   * @return
   */
  public static List<byte[]> split(byte[] array, byte[] delimiter) {
    final List<byte[]> byteArrays = new LinkedList<>();
    if (delimiter.length == 0) {
      // include the full original bytes
      byteArrays.add(array);
      return byteArrays;
    }
    int begin = 0;

    outer: for (int i = 0; i < array.length - delimiter.length + 1; i++) {
      for (int j = 0; j < delimiter.length; j++) {
        if (array[i + j] != delimiter[j]) {
          continue outer;
        }
      }

      // If delimiter is at the beginning then there will not be any data.
      if (begin != i) {
        byteArrays.add(Arrays.copyOfRange(array, begin, i));
      }
      begin = i + delimiter.length;
    }

    // delimiter at the very end with no data following?
    if (begin != array.length) {
      byteArrays.add(Arrays.copyOfRange(array, begin, array.length));
    }

    return byteArrays;
  }
}
