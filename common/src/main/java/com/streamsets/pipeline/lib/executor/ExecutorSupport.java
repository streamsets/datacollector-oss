package com.streamsets.pipeline.lib.executor;

import org.slf4j.Logger;
import org.slf4j.Marker;

public class ExecutorSupport {
  private Logger logger;

  public ExecutorSupport(Logger logger) {
    this.logger = logger;
  }

  public void uncaughtThrowableInRunnable(Throwable throwable, Runnable delegate, String delegateName) {
    String msg = "Uncaught throwable from " + delegateName + ": " + throwable;
    logger.error(msg, throwable);
  }
}
