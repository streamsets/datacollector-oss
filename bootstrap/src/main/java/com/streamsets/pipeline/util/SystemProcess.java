
/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface SystemProcess {
  public void start() throws IOException;
  public void start(Map<String, String> env) throws IOException;

  public String getCommand();

  public boolean isAlive();

  public void cleanup();

  public List<String> getAllOutput();

  public List<String> getAllError();

  public List<String> getOutput();

  public List<String> getError();

  public void kill(long timeoutBeforeForceKill) ;

  public int exitValue();

  public boolean waitFor(long timeout, TimeUnit unit)
    throws InterruptedException;
}
