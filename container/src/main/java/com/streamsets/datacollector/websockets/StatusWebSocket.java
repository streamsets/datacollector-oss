/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.websockets;

import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.dc.execution.manager.standalone.ThreadUsage;

import java.util.Queue;


public class StatusWebSocket extends BaseWebSocket implements StateEventListener{
  public static final String TYPE = "status";

  public StatusWebSocket(ListenerManager<StateEventListener> listenerManager, Queue<WebSocketMessage> queue) {
    super(TYPE, listenerManager, queue);
  }

  @Override
  public void onStateChange(PipelineState fromState, PipelineState toState, String toStateJson, ThreadUsage threadUsage) {

  }
}
