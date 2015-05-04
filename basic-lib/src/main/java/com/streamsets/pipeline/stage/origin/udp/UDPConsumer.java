/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.udp;


import io.netty.channel.socket.DatagramPacket;

public interface UDPConsumer {

  void process(DatagramPacket packet) throws Exception;
}
