/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
  private final ListenerManager listenerManager;
  private final Queue<WebSocketMessage> queue;
  private Session webSocketSession = null;


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