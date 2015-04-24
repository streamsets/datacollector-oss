/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.domainserver.main;

import com.streamsets.pipeline.http.WebServerTask;
import com.streamsets.pipeline.task.CompositeTask;
import com.streamsets.pipeline.task.Task;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;

public class DomainServerTask extends CompositeTask {

  @Inject
  public DomainServerTask(WebServerTask webServer) {
    super("domainServer", new ArrayList<Task>(Arrays.asList(webServer)), true);
  }

}
