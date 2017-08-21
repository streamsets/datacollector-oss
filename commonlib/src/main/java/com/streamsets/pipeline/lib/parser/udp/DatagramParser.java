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
package com.streamsets.pipeline.lib.parser.udp;

import com.google.common.io.ByteStreams;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DatagramMode;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.udp.UDPConstants;
import com.streamsets.pipeline.lib.udp.UDPMessage;
import com.streamsets.pipeline.lib.udp.UDPMessageDeserializer;
import io.netty.channel.socket.DatagramPacket;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Queue;

public class DatagramParser extends AbstractDataParser {

  public static final String PREFIX = "udp.";
  public static final String RECEIVED = PREFIX + "received";

  private static final UDPMessageDeserializer UDP_MESSAGE_DESERIALIZER = new UDPMessageDeserializer();

  private final InputStream is;
  private final AbstractParser datagramParser;
  private final DatagramMode datagramMode;

  // helps keep track of the current index of the records
  private boolean parsed = false;
  private Queue<Record> queue = null;
  private UDPMessage udpMessage = null;


  public DatagramParser(
    InputStream is,
    DatagramMode datagramMode,
    AbstractParser datagramParser
  ) {
    this.is = is;
    this.datagramMode = datagramMode;
    this.datagramParser = datagramParser;
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    if (!parsed) {
      // fresh data. parse and cache records
      byte[] bytes = ByteStreams.toByteArray(is);
      udpMessage = UDP_MESSAGE_DESERIALIZER.deserialize(bytes);
      DatagramPacket datagram = udpMessage.getDatagram();
      validateMessageType(udpMessage.getType());
      queue = new LinkedList<>();
      try {
        queue.addAll(datagramParser.parse(datagram.content(), datagram.recipient(), datagram.sender()));
      } catch (OnRecordErrorException e) {
        throw new DataParserException(e.getErrorCode(), e.getMessage());
      }
      parsed = true;
    }

    if (queue.isEmpty()) {
      // no more records to return, return null
      return null;
    }

    Record record = queue.poll();
    // set received time in record header
    record.getHeader().setAttribute(RECEIVED, String.valueOf(udpMessage.getReceived()));
    return record;
  }

  private void validateMessageType(int type) throws DataParserException {
    boolean valid = true;
    switch (type) {
      case UDPConstants.SYSLOG:
        if (datagramMode != DatagramMode.SYSLOG) {
          valid = false;
        }
        break;
      case UDPConstants.NETFLOW:
        if (datagramMode != DatagramMode.NETFLOW) {
          valid = false;
        }
        break;
      case UDPConstants.COLLECTD:
        if (datagramMode != DatagramMode.COLLECTD) {
          valid = false;
        }
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected UDP Message type {}", type));
    }
    if (!valid) {
      throw new DataParserException(Errors.DATAGRAM_00, datagramMode.getLabel(), getLabelForType(type));
    }

  }

  private Object getLabelForType(int type) {
    switch (type) {
      case UDPConstants.SYSLOG:
        return DatagramMode.SYSLOG.name();
      case UDPConstants.NETFLOW:
        return DatagramMode.NETFLOW.name();
      case UDPConstants.COLLECTD:
        return DatagramMode.COLLECTD.name();
      default:
        throw new IllegalStateException(Utils.format("Unexpected UDP Message type {}", type));
    }
  }

  @Override
  public String getOffset() {
    return String.valueOf("null");
  }

  @Override
  public void close() throws IOException {
    is.close();
  }

}
