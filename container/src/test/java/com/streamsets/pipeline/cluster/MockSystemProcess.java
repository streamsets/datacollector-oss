/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.streamsets.pipeline.util.SystemProcess;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MockSystemProcess implements SystemProcess {
  public static boolean isAlive = false;
  public static final List<String> output = new ArrayList<>();
  public static final List<String> error = new ArrayList<>();
  public static final List<String> args = new ArrayList<>();

  public static void reset() {
    isAlive = false;
    output.clear();
    error.clear();
    args.clear();
  }


  public MockSystemProcess(File tempDir, List<String> args) {
    String tempPrefix = tempDir.getParent();
    for (String arg : args) {
      arg = arg.replace(tempPrefix, "<masked>");
      MockSystemProcess.args.add(arg);
    }
  }

  @Override
  public void start() throws IOException {
    // do nothing
  }

  @Override
  public void start(Map<String, String> env) throws IOException {
    start();
  }

  @Override
  public boolean isAlive() {
    return isAlive;
  }

  @Override
  public Collection<String> getAllOutput() {
    return output;
  }

  @Override
  public Collection<String> getAllError() {
    return error;
  }

  @Override
  public List<String> getOutput() {
    return output;
  }

  @Override
  public Collection<String> getError() {
    return error;
  }

  @Override
  public String getCommand() {
    return "";
  }

  @Override
  public void cleanup() {
    // do nothing
  }

  @Override
  public int exitValue() {
    return 0;
  }

  @Override
  public void kill(long timeoutBeforeForceKill) {

  }

  @Override
  public boolean waitFor(long timeout, TimeUnit unit) {
    return true;
  }
}
