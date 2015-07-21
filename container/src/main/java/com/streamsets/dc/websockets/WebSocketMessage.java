/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.websockets;

import org.eclipse.jetty.websocket.api.Session;

import java.io.IOException;

public class WebSocketMessage {
  private final Session webSocketSession;
  private final String message;

  public WebSocketMessage(Session webSocketSession, String message) {
    this.webSocketSession = webSocketSession;
    this.message = message;
  }

  public void send() throws IOException {
    if(webSocketSession.isOpen()) {
      webSocketSession.getRemote().sendString(message);
    }
  }

}
