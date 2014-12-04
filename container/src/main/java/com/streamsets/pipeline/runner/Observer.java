/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.util.Configuration;

import java.util.List;
import java.util.Map;

public interface Observer {

  public void configure(Configuration conf);

  public boolean isObserving(Stage.Info info);

  public void observe(Pipe pipe, Map<String, List<Record>> snapshot);

}
