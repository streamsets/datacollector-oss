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
package com.streamsets.pipeline.lib.parser.udp.netflow;

import com.google.common.cache.Cache;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.net.netflow.BaseNetflowMessage;
import com.streamsets.pipeline.lib.parser.net.netflow.Errors;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowCommonDecoder;
import com.streamsets.pipeline.lib.parser.net.netflow.OutputValuesMode;
import com.streamsets.pipeline.lib.parser.net.netflow.v9.FlowSetTemplate;
import com.streamsets.pipeline.lib.parser.net.netflow.v9.FlowSetTemplateCacheKey;
import com.streamsets.pipeline.lib.parser.net.netflow.v9.NetflowV9Decoder;
import com.streamsets.pipeline.lib.parser.udp.AbstractParser;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by reading:
 * <a href="https://github.com/brockn/netflow">ASF licensed scala based netflow</a>,
 * <a href="http://www.cisco.com/en/US/technologies/tk648/tk362/technologies_white_paper09186a00800a3db9.html">v9 spec</a>,
 * and
 * <a href="http://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html#wp1003394">v1 and v5 spec</a>
 * <a href="http://www.cisco.com/en/US/technologies/tk648/tk362/technologies_white_paper09186a00800a3db9.html">v9</a>.
 */

public class NetflowParser extends AbstractParser {

  private final AtomicLong recordId;
  private final OutputValuesMode outputValuesMode;
  private final Cache<FlowSetTemplateCacheKey, FlowSetTemplate> flowSetTemplateCache;

  public NetflowParser(
      Stage.Context context,
      OutputValuesMode outputValuesMode,
      int maxTemplateCacheSize,
      int templateCacheTimeoutMs
  ) {
    super(context);
    recordId = new AtomicLong(0L);
    this.outputValuesMode = outputValuesMode;
    flowSetTemplateCache = NetflowV9Decoder.buildTemplateCache(maxTemplateCacheSize, templateCacheTimeoutMs);
  }

  public Record buildRecord(BaseNetflowMessage message) {
    Record record = context.createRecord(message.getReaderId() + "::" + recordId.getAndIncrement());
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
    final List<BaseNetflowMessage> messages = new LinkedList<>();
    final List<Record> records = new LinkedList<>();
    // create new instance to handle multithreading
    final NetflowCommonDecoder decoder = new NetflowCommonDecoder(
        outputValuesMode,
        // return instance of template cache held by this NetflowParser instance,
        // so it's shared across multiple invocations of parse
        // this is necessary because for this parser, we recreate the NetflowCommonDecoder
        // every time, which would otherwise wipe out the cache across multiple packets
        () -> flowSetTemplateCache
    );
    decoder.decodeStandaloneBuffer(buf, messages, sender, recipient);
    for (BaseNetflowMessage message : messages) {
      records.add(buildRecord(message));
    }
    return records;
  }
}
