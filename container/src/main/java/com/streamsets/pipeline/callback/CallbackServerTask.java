/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.callback;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

public class CallbackServerTask extends AbstractTask {
  private static final Logger LOG = LoggerFactory.getLogger(CallbackServerTask.class);
  private static final String CALLBACK_SERVER = "callbackServerTask";

  public static final String CALLBACK_SERVER_URL_KEY = "callback.server.url";
  public static final String CALLBACK_SERVER_URL_DEFAULT = null;
  public static final String CALLBACK_SERVER_PING_INTERVAL_KEY = "callback.server.ping.interval.ms";
  public static final Long CALLBACK_SERVER_PING_INTERVAL_DEFAULT = 5000l;
  public static final String SDC_CLUSTER_TOKEN_KEY = "sdc.cluster.token";

  private final RuntimeInfo runtimeInfo;
  private final Configuration conf;
  private SafeScheduledExecutorService executor;

  @Inject
  public CallbackServerTask(RuntimeInfo runtimeInfo, Configuration conf) {
    super(CALLBACK_SERVER);
    this.runtimeInfo = runtimeInfo;
    this.conf = conf;
  }

  @Override
  protected void initTask() {
    String domainServerURL = conf.get(CALLBACK_SERVER_URL_KEY, CALLBACK_SERVER_URL_DEFAULT);
    Long pingInterval = conf.get(CALLBACK_SERVER_PING_INTERVAL_KEY, CALLBACK_SERVER_PING_INTERVAL_DEFAULT);
    String sdcClusterToken = conf.get(SDC_CLUSTER_TOKEN_KEY, null);

    if(domainServerURL != null && runtimeInfo.getExecutionMode() == RuntimeInfo.ExecutionMode.SLAVE) {
      executor = new SafeScheduledExecutorService(1, CALLBACK_SERVER);
      CallbackServerRunnable domainServerCallbackRunnable = new CallbackServerRunnable(
        runtimeInfo, domainServerURL, sdcClusterToken);
      executor.scheduleAtFixedRate(domainServerCallbackRunnable, 0, pingInterval, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void stopTask() {
    LOG.debug("Stopping Callback Server Task");
    if(executor != null) {
      executor.shutdownNow();
    }
    LOG.debug("Stopped Callback Server Task");
  }

}
