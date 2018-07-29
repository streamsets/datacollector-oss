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
package com.streamsets.pipeline.stage.origin.salesforce;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.streamsets.pipeline.lib.salesforce.Errors;
import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSession;
import org.cometd.bayeux.client.ClientSession.Extension.Adapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForceReplayExtension<Type> extends Adapter {
  private static final Logger LOG = LoggerFactory.getLogger(ForceReplayExtension.class);

  private static final String REPLAY = "replay";

  private final Map<String, Type> map;
  private final BlockingQueue<Message> entityQueue;

  public ForceReplayExtension(Map<String, Type> map, BlockingQueue<Message> entityQueue) {
    this.map = new HashMap<>(map);
    this.entityQueue = entityQueue;
  }

  @Override
  public boolean rcv(ClientSession session, Message.Mutable message) {
    Object data = message.get(REPLAY);
    if (data != null) {
      try {
        map.put(message.getChannel(), (Type) data);
      } catch (ClassCastException e) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean rcvMeta(ClientSession session, Message.Mutable message) {
    switch (message.getChannel()) {
      case Channel.META_HANDSHAKE:
        // Does the server support replay?
        Map<String, Object> ext = message.getExt(false);
        if (ext == null || ext.get(REPLAY).equals(Boolean.FALSE)) {
          // Houston, we have a problem!
          try {
            entityQueue.put(message);
            return false;
          } catch (InterruptedException e) {
            LOG.error(Errors.FORCE_10.getMessage(), e);
            Thread.currentThread().interrupt();
          }
        }
    }
    return true;
  }

  @Override
  public boolean sendMeta(ClientSession session, Message.Mutable message) {
    switch (message.getChannel()) {
      case Channel.META_HANDSHAKE:
        // We want to do replay!
        message.getExt(true).put(REPLAY, Boolean.TRUE);
        break;
      case Channel.META_SUBSCRIBE:
        // Send the last replay ID
        message.getExt(true).put(REPLAY, map);
        break;
    }
    return true;
  }
}
