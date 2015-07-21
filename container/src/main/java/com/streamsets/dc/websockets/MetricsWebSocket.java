/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.websockets;

import com.streamsets.pipeline.metrics.MetricsEventListener;

import java.util.Queue;

public class MetricsWebSocket extends BaseWebSocket implements MetricsEventListener {
  public static final String TYPE = "metrics";

  public MetricsWebSocket(ListenerManager<MetricsEventListener> listenerManager, Queue<WebSocketMessage> queue) {
    super(TYPE, listenerManager, queue);
  }

}