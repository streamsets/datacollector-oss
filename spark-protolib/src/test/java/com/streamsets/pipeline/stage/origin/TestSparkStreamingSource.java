/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;


import com.google.common.io.Resources;
import com.streamsets.pipeline.BootstrapSpark;
import com.streamsets.pipeline.main.EmbeddedPipelineFactory;
import com.streamsets.pipeline.stage.origin.spark.SparkStreamingBinding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestSparkStreamingSource {
  private static final Logger LOG = LoggerFactory.getLogger(TestSparkStreamingSource.class);
  private TextServer textServer;

  @Before
  public void setup() throws Exception {
    System.setProperty("sdc.testing-mode", "true");
    System.setProperty("spark.master", "local[2]"); // must be 2, not 1 (or function will never be called)
                                                    // not 3 (due to metric counter being jvm wide)
    textServer = new TextServer("msg");
    textServer.start();
    File target = new File(System.getProperty("user.dir"), "target");
    Properties properties = new Properties();
    properties.setProperty(EmbeddedPipelineFactory.PIPELINE_NAME, "pipeline1");
    properties.setProperty(EmbeddedPipelineFactory.PIPELINE_USER, "admin");
    properties.setProperty(EmbeddedPipelineFactory.PIPELINE_DESCRIPTION, "not much to say");
    properties.setProperty(EmbeddedPipelineFactory.PIPELINE_TAG, "unused");
    properties.setProperty(SparkStreamingBinding.INPUT_TYPE, SparkStreamingBinding.TEXT_SERVER_INPUT_TYPE);
    properties.setProperty(SparkStreamingBinding.TEXT_SERVER_HOSTNAME, "localhost");
    properties.setProperty(SparkStreamingBinding.TEXT_SERVER_PORT, String.valueOf(textServer.getPort()));
    File propertiesFile = new File(target, "sdc.properties");
    propertiesFile.delete();
    properties.store(new FileOutputStream(propertiesFile), null);
    File pipelineJson = new File(target, "pipeline.json");
    pipelineJson.delete();
    Files.copy(Paths.get(Resources.getResource("SIMPLE_TRASH_PIPELINE.json").toURI()),
      pipelineJson.toPath());
  }

  @After
  public void tearDown() throws Exception {
    if (textServer != null) {
      textServer.stop();
    }
  }

  @Test
  public void test() throws Exception {
    Thread waiter = new Thread() {
      public void run() {
        try {
          BootstrapSpark.main(new String[0]);
        } catch (IllegalStateException ex) {
          // ignored
        } catch (Exception ex) {
          LOG.error("Error in waiter thread: " + ex, ex);
        }
      }
    };
    waiter.setName(getClass().getName() + "-Waiter");
    waiter.setDaemon(true);
    waiter.start();
    try {
      TimeUnit.SECONDS.sleep(60);
      long actual = SparkExecutorFunction.getRecordsProducedJVMWide();
      Assert.assertTrue("Expected between 40 and 70 but was: " + actual, actual >= 40 && actual <= 70);
    } finally {
      waiter.interrupt();;
    }
  }
}
