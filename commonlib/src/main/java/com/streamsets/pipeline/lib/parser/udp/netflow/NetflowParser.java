/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.parser.udp.netflow;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.net.netflow.Errors;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowDecoder;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowMessage;
import com.streamsets.pipeline.lib.parser.udp.AbstractParser;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by reading:
 * <a href="https://github.com/brockn/netflow">ASF licensed scala based netflow</a>,
 * <a href="http://www.cisco.com/en/US/technologies/tk648/tk362/technologies_white_paper09186a00800a3db9.html">v9 spec</a>,
 * and
 * <a href="http://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html#wp1003394">v1 and v5 spec</a>
 * <a href="http://www.cisco.com/en/US/technologies/tk648/tk362/technologies_white_paper09186a00800a3db9.html">v9</a>.
 */

public class NetflowParser extends AbstractParser {

  private long recordId;

  public NetflowParser(Stage.Context context) {
    super(context);
    this.recordId = 0;
  }

  public Record buildRecord(NetflowMessage message) {
    Record record = context.createRecord(message.getReaderId() + "::" + recordId++);
    message.populateRecord(record);
    return record;
  }

  @Override
  public List<Record> parse(ByteBuf buf, InetSocketAddress recipient, InetSocketAddress sender)
      throws OnRecordErrorException {
    int packetLength = buf.readableBytes();
    // need to keep this check here for UDP only, since we expect the entire packet to be readable at once
    if (packetLength < 4) {
      throw new OnRecordErrorException(
          Errors.NETFLOW_01,
          Utils.format("Packet must be at least 4 bytes, was: {}", packetLength)
      );
    }
    final List<NetflowMessage> messages = new LinkedList<>();
    final List<Record> records = new LinkedList<>();
    // create new instance to handle multithreading
    new NetflowDecoder().decodeStandaloneBuffer(buf, messages, sender, recipient);
    for (NetflowMessage message : messages) {
      records.add(buildRecord(message));
    }
    return records;
  }
}
