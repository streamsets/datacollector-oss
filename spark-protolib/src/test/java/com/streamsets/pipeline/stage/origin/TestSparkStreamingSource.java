/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.BlackListURLClassLoader;
import com.streamsets.pipeline.BootstrapMain;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.restapi.bean.PipelineConfigurationJson;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestSparkStreamingSource {

  private TextServer textServer;
  private SparkStreamingBinding sparkStreamingSource;

  @Before
  public void setup() throws Exception {
    textServer = new TextServer("msg");
    textServer.start();
    List<URL> urls = new ArrayList<>();
    urls.addAll(BootstrapMain.getClasspathUrls(System.getProperty("user.dir") + "/target/"));
    ObjectMapper json = ObjectMapperFactory.get();
    File pipelineFile = new File("/tmp/test.json"); // TODO we need a pipline json file, it should have a source of
    // SparkStreamingSource and a destination of a *new* dev null source which is also in the spark-streaming
    // jar. Once we figure out the classloader stuff we can use the dev null source inside basic-lib
    String rawConfig = json.writeValueAsString(json.readValue(pipelineFile, Map.class).get("pipelineConfig"));
    PipelineConfigurationJson pipelineConfigBean = json.readValue(rawConfig, PipelineConfigurationJson.class);
    EmbeddedPipeline pipeline = new EmbeddedPipeline("test", "admin", "tag1", "what is this?",
      pipelineConfigBean);
    EmbeddedSDCConf sdcConf = new EmbeddedSDCConf(pipeline, urls);
    sparkStreamingSource = new SparkStreamingBinding("local[2]", "localhost",
      textServer.getPort(), sdcConf);
    sparkStreamingSource.init();
  }

  @After
  public void tearDown() throws Exception {
    if (textServer != null) {
      textServer.stop();
    }
    if (sparkStreamingSource != null) {
      sparkStreamingSource.destroy();
    }
  }


  @Test
  public void test() throws Exception {
    TimeUnit.SECONDS.sleep(20);
    long actual = SparkExecutorFunction.getRecordsProducedJVMWide();
    Assert.assertTrue("Expected between 7 and 11 but was: " + actual, actual >= 7 && actual <= 11);
  }
}
