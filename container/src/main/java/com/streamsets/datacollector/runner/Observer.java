/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.streamsets.datacollector.runner.production.RulesConfigurationChangeRequest;
import com.streamsets.pipeline.api.Record;

import java.util.List;
import java.util.Map;

public interface Observer {

  public void reconfigure();

  public boolean isObserving(List<String> lanes);

  public void observe(Pipe pipe, Map<String, List<Record>> snapshot);

  public void setConfiguration(RulesConfigurationChangeRequest rulesConfigurationChangeRequest);

}
