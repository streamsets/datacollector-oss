/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.domainServer;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

public class DomainServerCallbackTask extends AbstractTask {
  private static final Logger LOG = LoggerFactory.getLogger(DomainServerCallbackTask.class);
  private static final String DOMAIN_SERVER_CALLBACK = "domainServerCallback";

  public static final String DOMAIN_SERVER_URL_KEY = "domain.server.url";
  public static final String DOMAIN_SERVER_URL_DEFAULT = null;
  public static final String DOMAIN_SERVER_PING_INTERVAL_KEY = "domain.server.ping.interval.ms";
  public static final Long DOMAIN_SERVER_PING_INTERVAL_DEFAULT = 15000l;

  private final RuntimeInfo runtimeInfo;
  private final Configuration conf;
  private SafeScheduledExecutorService executor;

  @Inject
  public DomainServerCallbackTask(RuntimeInfo runtimeInfo, Configuration conf) {
    super(DOMAIN_SERVER_CALLBACK);
    this.runtimeInfo = runtimeInfo;
    this.conf = conf;
  }

  @Override
  protected void initTask() {
    String domainServerURL = conf.get(DOMAIN_SERVER_URL_KEY, DOMAIN_SERVER_URL_DEFAULT);
    Long pingInterval = conf.get(DOMAIN_SERVER_PING_INTERVAL_KEY, DOMAIN_SERVER_PING_INTERVAL_DEFAULT);

    if(domainServerURL != null) {
      executor = new SafeScheduledExecutorService(1, DOMAIN_SERVER_CALLBACK);
      DomainServerCallbackRunnable domainServerCallbackRunnable = new DomainServerCallbackRunnable(
        runtimeInfo, domainServerURL);
      executor.scheduleAtFixedRate(domainServerCallbackRunnable, 0, pingInterval, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void stopTask() {
    LOG.debug("Stopping Domain Server Callback Task");
    if(executor != null) {
      executor.shutdown();
      try {
        executor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        executor.shutdownNow();
        LOG.warn(Utils.format("Forced termination. Reason {}", e.getMessage()));
      }
    }
    LOG.debug("Stopped Domain Server Callback Task");
  }


}
