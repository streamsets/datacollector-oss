/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.websockets;

import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.prodmanager.StateEventListener;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;


public class StatusWebSocket extends WebSocketAdapter implements StateEventListener{
  public static final String TYPE = "status";
  private final static Logger LOG = LoggerFactory.getLogger(StatusWebSocket.class);
  private final ProductionPipelineManagerTask pipelineManager;
  private Session webSocketSession = null;


  public StatusWebSocket(ProductionPipelineManagerTask pipelineManager) {
    this.pipelineManager = pipelineManager;
  }

  @Override
  public void onWebSocketConnect(final Session session) {
    super.onWebSocketConnect(session);
    pipelineManager.addStateEventListener(this);
    webSocketSession = session;
  }

  @Override
  public void onWebSocketClose(int statusCode, String reason) {
    super.onWebSocketClose(statusCode, reason);
    pipelineManager.removeStateEventListener(this);
    SDCWebSocketServlet.webSocketClients--;
    webSocketSession = null;
  }

  @Override
  public void onWebSocketError(Throwable cause) {
    super.onWebSocketError(cause);
    LOG.warn("MetricsWebSocket error: {}", cause.getMessage(), cause);
    pipelineManager.removeStateEventListener(this);
    SDCWebSocketServlet.webSocketClients--;
    webSocketSession = null;
  }

  @Override
  public void notification(String pipelineStateJSONStr) {
    try {
      if(webSocketSession != null) {
        webSocketSession.getRemote().sendString(pipelineStateJSONStr);
      }
    } catch (IOException ex) {
      LOG.warn("Error while sending pipeline status through WebSocket message, {}", ex.getMessage(), ex);
    }
  }

}