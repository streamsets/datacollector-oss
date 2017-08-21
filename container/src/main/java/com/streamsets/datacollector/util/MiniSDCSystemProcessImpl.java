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
package com.streamsets.datacollector.util;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

// TODO - move this and subclass to bootstrap
public class MiniSDCSystemProcessImpl extends SystemProcessImpl {
  private static final Logger LOG = LoggerFactory.getLogger(SystemProcessImpl.class);
  public static final String YARN_STATUS_SUCCESS = " State : RUNNING ";
  private static final String YARN_COMMAND_TEXT = Joiner.on("\n").join("#!/bin/bash", "echo \"$@\"", "echo '",
    YARN_STATUS_SUCCESS, "'", "");
  private final File testDir;
  private final String yarnCommand;

  public MiniSDCSystemProcessImpl(String name, File tempDir, List<String> args, File testDir) {
    super(name, tempDir, testDir);
    if (args.contains("start")) {
      int index = args.indexOf("--class");
      // add properties file for yarn configs for mini test cases
      args.add(index, "--properties-file");
      LOG.debug("Spark property file is at " + System.getProperty("SPARK_PROPERTY_FILE"));
      args.add(index + 1, System.getProperty("SPARK_PROPERTY_FILE"));
    }
    File yarnCommand = new File(tempDir, "yarn-command");
    if (!yarnCommand.isFile() || !yarnCommand.canExecute()) {
      if (!yarnCommand.delete()) {
        LOG.warn("Failed to delete yarn command file " + yarnCommand);
      }
      try {
        Files.write(YARN_COMMAND_TEXT, yarnCommand, StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (!yarnCommand.canExecute() && !yarnCommand.setExecutable(true)) {
        throw new RuntimeException("Could not set " + yarnCommand + " executable");
      }
    }
    this.yarnCommand = yarnCommand.getAbsolutePath();
    this.args = ImmutableList.copyOf(args);
    this.testDir = testDir;
  }

  @Override
  public void start(Map<String, String> env) throws IOException {
    String sparkHomeDir = System.getProperty("SPARK_TEST_HOME");
    LOG.debug("Spark home in test case is at " + sparkHomeDir);
    env.put("SPARK_SUBMIT_YARN_COMMAND", new File(sparkHomeDir, "bin/spark-submit").getAbsolutePath());
    // need to set some this prop, actual value doesn't matter
    env.put("YARN_CONF_DIR", testDir.getAbsolutePath());
    env.put("YARN_COMMAND",  yarnCommand);
    super.start(env);
  }

}
