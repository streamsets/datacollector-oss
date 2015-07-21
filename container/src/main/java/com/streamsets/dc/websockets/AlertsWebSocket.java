/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.websockets;

import com.streamsets.pipeline.alerts.AlertEventListener;
import java.util.Queue;


public class AlertsWebSocket extends BaseWebSocket implements AlertEventListener {
  public static final String TYPE = "alerts";

  public AlertsWebSocket(ListenerManager<AlertEventListener> listenerManager, Queue<WebSocketMessage> queue) {
    super(TYPE, listenerManager, queue);
  }

}