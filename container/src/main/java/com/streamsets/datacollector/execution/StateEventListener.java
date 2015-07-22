/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution;

import com.streamsets.dc.execution.manager.standalone.ThreadUsage;

import java.util.EventListener;

public interface StateEventListener extends EventListener {

  void onStateChange(PipelineState fromState, PipelineState toState, String toStateJson, ThreadUsage threadUsage);

}
