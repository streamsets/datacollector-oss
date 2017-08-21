/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.cluster;

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
  public static Map<String, String> env;

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
    MockSystemProcess.env = env;
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
