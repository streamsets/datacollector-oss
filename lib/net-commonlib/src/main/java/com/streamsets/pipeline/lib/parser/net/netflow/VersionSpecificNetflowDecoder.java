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

package com.streamsets.pipeline.lib.parser.net.netflow;

import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.net.MessageToRecord;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.util.List;

public interface VersionSpecificNetflowDecoder <MT extends MessageToRecord> {

  /**
   * Resets the state of the decoder. Called when the parent decoder encounters an error or we have finished processing
   * the entire flow.
   */
  void resetState();

  /**
   * Parse the {@link ByteBuf} and extract relevant Netflow messages from it. Returns a list when the entire set of
   * flows has been read from the buffer. This method may be called multiple times as per the semantics of the
   * {@link io.netty.handler.codec.ReplayingDecoder}. The parent decoder (which parses the Netflow message netflowVersion)
   * is solely responsible for managing the position of the {@link ByteBuf} w.r.t. ReplayingDecoder semantics. In
   * other words, the implementing class here need not worry about doing anything besides attempting to read bytes,
   * calling {@link NetflowCommonDecoder#doCheckpoint()} on the parent decoder when it wants to record its state, and
   * returning the List only when it has finished decoding.
   *
   * @param netflowVersion the Netflow netflowVersion, should match the netflowVersion expected by this decoder instance
   * @param packetLength the length of the packet; only used when whole packet is sent at once (ex: UDP)
   * @param packetLengthCheck whether to perform a packet length check (i.e. that the readable bytes from the
   *     given {@link ByteBuf} param is sufficient given the length of packet expected by packetLength)
   * @param buf the {@link ByteBuf} that contains the Netflow message data
   * @param sender the address of the sender (might be null if undetermined)
   * @param recipient the address of the recipient (might be null if undetermined)
   */
  List<MT> parse(
      int netflowVersion,
      int packetLength,
      boolean packetLengthCheck,
      ByteBuf buf,
      InetSocketAddress sender,
      InetSocketAddress recipient
  ) throws OnRecordErrorException;
}
