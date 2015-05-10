/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

// TODO - move this and subclass to bootstrap
public class MiniSDCSystemProcessImpl extends SystemProcessImpl {
  private static final Logger LOG = LoggerFactory.getLogger(SystemProcessImpl.class);
  private final File testDir;

  public MiniSDCSystemProcessImpl(String name, File tempDir, List<String> args, File testDir) {
    super(name, tempDir, testDir);
    if (args.contains("start")) {
      int index = args.indexOf("--class");
      // add properties file for yarn configs for mini test cases
      args.add(index, "--properties-file");
      LOG.debug("Spark property file is at " + System.getProperty("SPARK_PROPERTY_FILE"));
      args.add(index + 1, System.getProperty("SPARK_PROPERTY_FILE"));
    }
    this.args = ImmutableList.copyOf(args);
    this.testDir = testDir;
  }

  @Override
  public void start(Map<String, String> env) throws IOException {
    String sparkHomeDir = System.getProperty("SPARK_TEST_HOME");
    LOG.debug("Spark home in test case is at " + sparkHomeDir);
    env.put("SPARK_HOME", sparkHomeDir);
    env.put("SPARK_SUBMIT", new File(sparkHomeDir, "bin/spark-submit").getAbsolutePath());
    // need to set some this prop, actual value doesn't matter
    env.put("YARN_CONF_DIR", testDir.getAbsolutePath());
    super.start(env);
  }

}
