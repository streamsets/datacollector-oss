/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import java.util.EventListener;

public interface AlertEventListener extends EventListener {

  void notification(String ruleDefinition);

}
