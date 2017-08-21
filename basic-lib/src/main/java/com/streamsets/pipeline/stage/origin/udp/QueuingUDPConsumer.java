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
package com.streamsets.pipeline.stage.origin.udp;


import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.udp.AbstractParser;
import com.streamsets.pipeline.lib.udp.UDPConsumer;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class QueuingUDPConsumer implements UDPConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(QueuingUDPConsumer.class);
  private final AbstractParser parser;
  private final BlockingQueue<ParseResult> queue;
  private final AtomicLong totalPackets;
  private final AtomicLong droppedPackets;

  public QueuingUDPConsumer(AbstractParser parser, BlockingQueue<ParseResult> queue) {
    this.parser = parser;
    this.queue = queue;
    this.droppedPackets = new AtomicLong(0);
    this.totalPackets = new AtomicLong(0);
  }

  @Override
  public void process(DatagramPacket packet) throws Exception {
    long total = totalPackets.incrementAndGet();
    boolean droppedPacket = false;
    ParseResult result;
    try {
      List<Record> records = parser.parse(packet.content(), packet.recipient(), packet.sender());
      result = new ParseResult(records);
    } catch (OnRecordErrorException ex) {
      result = new ParseResult(ex);
    }
    if (!queue.offer(result)) {
      droppedPacket = true;
      long dropped = droppedPackets.incrementAndGet();
      if (dropped % 1000 == 0) {
        LOG.info("Could not add packet to queue, dropped {} of {} packets", dropped, total);
      }
    }
    if (!droppedPacket && total % 1000 == 0) {
      LOG.info("Consumed {} total packets", total);
    }
  }
}
