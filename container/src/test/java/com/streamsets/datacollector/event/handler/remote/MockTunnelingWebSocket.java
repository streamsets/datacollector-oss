/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.event.handler.remote;

import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.tunneling.TunnelingResponse;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;

import java.io.IOException;

public class MockTunnelingWebSocket extends WebSocketAdapter {
  public Session session;
  public TunnelingResponse lastMessage;

  @Override
  public void onWebSocketBinary(byte[] payload, int offset, int len) {
    try {
      lastMessage = ObjectMapperFactory.get().readValue(payload, offset, len, TunnelingResponse.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
    super.onWebSocketBinary(payload, offset, len);
  }

  @Override
  public void onWebSocketConnect(Session sess) {
    this.session = sess;
    super.onWebSocketConnect(sess);
  }
}
