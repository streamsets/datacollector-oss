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
import java.util.Queue;


public class AlertsWebSocket extends BaseWebSocket implements AlertEventListener {
  public static final String TYPE = "alerts";

  public AlertsWebSocket(ListenerManager<AlertEventListener> listenerManager, Queue<WebSocketMessage> queue) {
    super(TYPE, listenerManager, queue);
  }

}