/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.udp;


import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.AbstractParser;
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
