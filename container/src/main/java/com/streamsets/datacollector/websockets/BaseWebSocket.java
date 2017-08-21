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
package com.streamsets.datacollector.websockets;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

public class BaseWebSocket extends WebSocketAdapter {
  private final static Logger LOG = LoggerFactory.getLogger(BaseWebSocket.class);

  private final String type;
  private final ListenerManager<Object> listenerManager;
  private final Queue<WebSocketMessage> queue;
  private Session webSocketSession = null;


  @SuppressWarnings("unchecked")
  public BaseWebSocket(String type, ListenerManager listenerManager, Queue<WebSocketMessage> queue) {
    this.type = type;
    this.listenerManager = listenerManager;
    this.queue = queue;
  }

  @Override
  public void onWebSocketConnect(final Session session) {
    super.onWebSocketConnect(session);
    listenerManager.register(this);
    webSocketSession = session;
  }

  @Override
  public void onWebSocketClose(int statusCode, String reason) {
    super.onWebSocketClose(statusCode, reason);
    listenerManager.unregister(this);
    SDCWebSocketServlet.webSocketClients--;
    webSocketSession = null;
  }

  @Override
  public void onWebSocketError(Throwable cause) {
    super.onWebSocketError(cause);
    LOG.warn("WebSocket '{}' error: {}", type, cause.toString(), cause);
    listenerManager.unregister(this);
    SDCWebSocketServlet.webSocketClients--;
    webSocketSession = null;
  }

  public void notification(String message) {
    if(webSocketSession != null && webSocketSession.isOpen()) {
      if (!queue.offer(new WebSocketMessage(webSocketSession, message))) {
        LOG.warn("WebSocket queue is full, discarding '{}' message", type);
      }
    }
  }

}
