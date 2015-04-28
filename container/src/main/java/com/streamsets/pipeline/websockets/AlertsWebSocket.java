/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.websockets;

import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class AlertsWebSocket extends WebSocketAdapter implements AlertEventListener{
  public static final String TYPE = "alerts";
  private final static Logger LOG = LoggerFactory.getLogger(AlertsWebSocket.class);
  private final PipelineManager pipelineManager;
  private Session webSocketSession = null;


  public AlertsWebSocket(PipelineManager pipelineManager) {
    this.pipelineManager = pipelineManager;
  }

  @Override
  public void onWebSocketConnect(final Session session) {
    super.onWebSocketConnect(session);
    pipelineManager.addAlertEventListener(this);
    webSocketSession = session;
  }

  @Override
  public void onWebSocketClose(int statusCode, String reason) {
    super.onWebSocketClose(statusCode, reason);
    pipelineManager.removeAlertEventListener(this);
    SDCWebSocketServlet.webSocketClients--;
    webSocketSession = null;
  }

  @Override
  public void onWebSocketError(Throwable cause) {
    super.onWebSocketError(cause);
    LOG.warn("MetricsWebSocket error: {}", cause.getMessage(), cause);
    pipelineManager.removeAlertEventListener(this);
    SDCWebSocketServlet.webSocketClients--;
    webSocketSession = null;
  }

  @Override
  public void notification(String ruleDefinition) {
    try {
      if(webSocketSession != null) {
        webSocketSession.getRemote().sendString(ruleDefinition);
      }
    } catch (IOException ex) {
      LOG.warn("Error while sending alerts through WebSocket message, {}", ex.getMessage(), ex);
    }
  }

}