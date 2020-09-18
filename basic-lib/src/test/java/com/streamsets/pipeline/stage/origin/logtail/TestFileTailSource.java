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
package com.streamsets.pipeline.stage.origin.logtail;

import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.FileRollMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.io.GlobFileContextProvider;
import com.streamsets.pipeline.lib.parser.log.Constants;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({
    "javax.management.*",
    "jdk.internal.reflect.*"
})
@PrepareForTest({FileTailSource.class, GlobFileContextProvider.class})
public class TestFileTailSource {
  private final static int SCAN_INTERVAL = 0; //using zero forces synchronous file discovery
  private final Logger LOGGER = LoggerFactory.getLogger(TestFileTailSource.class);

  @Test(expected = StageException.class)
  //Non existing directory with conf.allowLateDirectory=false (default) will throw exception.
  public void testInitDirDoesNotExist() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/logFile.txt";
    fileInfo.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane").addOutputLane("metadata")
        .build();
    runner.runInit();
    runner.runDestroy();
  }

  @Test
  public void testTailLog() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    String logFile = new File(testDataDir, "logFile.txt").getAbsolutePath();
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("testLogFile.txt");
    OutputStream os = new FileOutputStream(logFile);
    IOUtils.copy(is, os);
    is.close();

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/logFile.txt";
    fileInfo.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane").addOutputLane("metadata")
        .build();
    runner.runInit();
    os.write("HELLO\n".getBytes());
    os.close();
    try {
      long start = System.currentTimeMillis();
      StageRunner.Output output = runner.runProduce(null, 1000);
      long end = System.currentTimeMillis();
      Assert.assertTrue(end - start >= 1000);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(3, output.getRecords().get("lane").size());
      Record record = output.getRecords().get("lane").get(0);
      Assert.assertEquals("FIRST", record.get("/text").getValueAsString());
      Assert.assertEquals(fileInfo.fileFullPath, record.getHeader().getAttribute(HeaderAttributeConstants.FILE));
      Assert.assertEquals("logFile.txt", record.getHeader().getAttribute(HeaderAttributeConstants.FILE_NAME));
      Assert.assertEquals("0", record.getHeader().getAttribute(HeaderAttributeConstants.OFFSET));
      Assert.assertEquals(
        String.valueOf(Files.getLastModifiedTime(Paths.get(fileInfo.fileFullPath)).toMillis()),
        record.getHeader().getAttribute(HeaderAttributeConstants.LAST_MODIFIED_TIME)
      );
      record = output.getRecords().get("lane").get(1);
      Assert.assertEquals(fileInfo.fileFullPath, record.getHeader().getAttribute(HeaderAttributeConstants.FILE));
      Assert.assertEquals("logFile.txt", record.getHeader().getAttribute(HeaderAttributeConstants.FILE_NAME));
      Assert.assertEquals("6", record.getHeader().getAttribute(HeaderAttributeConstants.OFFSET));
      Assert.assertEquals(
        String.valueOf(Files.getLastModifiedTime(Paths.get(fileInfo.fileFullPath)).toMillis()),
        record.getHeader().getAttribute(HeaderAttributeConstants.LAST_MODIFIED_TIME)
      );
      Assert.assertEquals("LAST", record.get("/text").getValueAsString());
      record = output.getRecords().get("lane").get(2);
      Assert.assertEquals("HELLO", record.get("/text").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  private static final Charset UTF8 = StandardCharsets.UTF_8;

  @Test
  public void testTailLogSameFilesInSameDir() throws Exception {
    File testDataDir1 = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir1.mkdirs());
    Files.write(new File(testDataDir1, "log1.txt").toPath(), Arrays.asList("Hello"), UTF8);
    Files.write(new File(testDataDir1, "log1.txt").toPath(), Arrays.asList("Hola"), UTF8);

    FileInfo fileInfo1 = new FileInfo();
    fileInfo1.fileFullPath = testDataDir1.getAbsolutePath() + "/log1.txt";
    fileInfo1.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo1.firstFile = "";
    fileInfo1.patternForToken = "";
    FileInfo fileInfo2 = new FileInfo();
    fileInfo2.fileFullPath = testDataDir1.getAbsolutePath() + "/log1.txt";
    fileInfo2.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo2.firstFile = "";
    fileInfo2.patternForToken = "";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo1, fileInfo2);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane").addOutputLane("metadata")
        .build();
    Assert.assertTrue(!runner.runValidateConfigs().isEmpty());
    Assert.assertTrue(runner.runValidateConfigs().get(0).toString().contains("TAIL_04"));
  }

  @Test
  public void testTailLogMultipleDirs() throws Exception {
    File testDataDir1 = new File("target", UUID.randomUUID().toString());
    File testDataDir2 = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir1.mkdirs());
    Assert.assertTrue(testDataDir2.mkdirs());
    Files.write(new File(testDataDir1, "log1.txt").toPath(), Arrays.asList("Hello"), UTF8);
    Files.write(new File(testDataDir2, "log2.txt").toPath(), Arrays.asList("Hola"), UTF8);

    FileInfo fileInfo1 = new FileInfo();
    fileInfo1.tag = "tag1";
    fileInfo1.fileFullPath = testDataDir1.getAbsolutePath() + "/log1.txt";
    fileInfo1.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo1.firstFile = "";
    fileInfo1.patternForToken = "";
    FileInfo fileInfo2 = new FileInfo();
    fileInfo2.tag = "";
    fileInfo2.fileFullPath = testDataDir2.getAbsolutePath() + "/log2.txt";
    fileInfo2.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo2.firstFile = "";
    fileInfo2.patternForToken = "";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo1, fileInfo2);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane").addOutputLane("metadata")
        .build();
    runner.runInit();
    try {
      Date now = new Date();
      Thread.sleep(10);
      StageRunner.Output output = runner.runProduce(null, 1000);
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Record record = output.getRecords().get("lane").get(0);
      Assert.assertEquals("Hello", record.get("/text").getValueAsString());
      Assert.assertEquals("tag1", record.getHeader().getAttribute("tag"));
      Assert.assertEquals(fileInfo1.fileFullPath, record.getHeader().getAttribute("file"));
      record = output.getRecords().get("lane").get(1);
      Assert.assertEquals("Hola", record.get("/text").getValueAsString());
      Assert.assertNull(record.getHeader().getAttribute("tag"));
      Assert.assertEquals(fileInfo2.fileFullPath, record.getHeader().getAttribute("file"));

      Assert.assertEquals(2, output.getRecords().get("metadata").size());
      Record metadata = output.getRecords().get("metadata").get(0);
      Assert.assertEquals(new File(fileInfo1.fileFullPath).getAbsolutePath(),
          metadata.get("/fileName").getValueAsString());
      Assert.assertEquals("START", metadata.get("/event").getValueAsString());
      Assert.assertTrue(now.compareTo(metadata.get("/time").getValueAsDate()) < 0);
      Assert.assertTrue(metadata.has("/inode"));
      metadata = output.getRecords().get("metadata").get(1);
      Assert.assertEquals(new File(fileInfo2.fileFullPath).getAbsolutePath(),
          metadata.get("/fileName").getValueAsString());
      Assert.assertEquals("START", metadata.get("/event").getValueAsString());
      Assert.assertTrue(now.compareTo(metadata.get("/time").getValueAsDate()) < 0);
      Assert.assertTrue(metadata.has("/inode"));

      List<EventRecord> eventRecords = runner.getEventRecords();
      Assert.assertNotNull(eventRecords);
      Assert.assertEquals(2, eventRecords.size());

      metadata = eventRecords.get(0);
      Assert.assertEquals(new File(fileInfo1.fileFullPath).getAbsolutePath(),
          metadata.get("/fileName").getValueAsString());
      Assert.assertEquals("START", metadata.get("/event").getValueAsString());
      Assert.assertTrue(now.compareTo(metadata.get("/time").getValueAsDate()) < 0);
      Assert.assertTrue(metadata.has("/inode"));

      metadata = eventRecords.get(1);
      Assert.assertEquals(new File(fileInfo2.fileFullPath).getAbsolutePath(),
          metadata.get("/fileName").getValueAsString());
      Assert.assertEquals("START", metadata.get("/event").getValueAsString());
      Assert.assertTrue(now.compareTo(metadata.get("/time").getValueAsDate()) < 0);
      Assert.assertTrue(metadata.has("/inode"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTailLogOffset() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    String logFile = new File(testDataDir, "logFile.txt").getAbsolutePath();
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("testLogFile.txt");
    OutputStream os = new FileOutputStream(logFile);
    IOUtils.copy(is, os);
    is.close();
    os.close();

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/logFile.txt";
    fileInfo.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 1;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 7;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane").addOutputLane("metadata")
        .build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Record record = output.getRecords().get("lane").get(0);
      Assert.assertEquals("FIRST", record.get("/text").getValueAsString());

      String offset = output.getNewOffset();
      Assert.assertNotNull(offset);
      output = runner.runProduce(offset, 1000);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      record = output.getRecords().get("lane").get(0);
      Assert.assertEquals("LAST", record.get("/text").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTailLogTruncated() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    String logFile = new File(testDataDir, "logFile.txt").getAbsolutePath();
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("testLogFileTruncated.txt");
    OutputStream os = new FileOutputStream(logFile);
    IOUtils.copy(is, os);
    is.close();
    os.close();

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/logFile.txt";
    fileInfo.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 1;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    //Set max line length of 2 characters
    conf.dataFormatConfig.textMaxLineLen = 3;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane").addOutputLane("metadata")
        .build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Record record = output.getRecords().get("lane").get(0);
      //max line length was set to 2 characters. So text has just 3 chars and there is also a boolean field truncated = true
      Assert.assertEquals("FIR", record.get("/text").getValueAsString());
      Assert.assertEquals(true, record.get("/truncated").getValueAsBoolean());

      String offset = output.getNewOffset();
      Assert.assertNotNull(offset);
      output = runner.runProduce(offset, 1000);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      record = output.getRecords().get("lane").get(0);
      //max line length was set to 2 characters. So text has just 3 chars and there is also a boolean field truncated = true
      Assert.assertEquals("LAS", record.get("/text").getValueAsString());
      Assert.assertEquals(true, record.get("/truncated").getValueAsBoolean());

      offset = output.getNewOffset();
      Assert.assertNotNull(offset);
      output = runner.runProduce(offset, 1000);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      record = output.getRecords().get("lane").get(0);
      Assert.assertEquals("NA", record.get("/text").getValueAsString());
      //No truncated field as there is no truncation
      Assert.assertEquals(false, record.has("/truncated"));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTailJson() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    File logFile = new File(testDataDir, "logFile.txt");

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/logFile.txt";
    fileInfo.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.JSON;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.jsonMaxObjectLen = 1024;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane").addOutputLane("metadata")
        .build();
    runner.runInit();
    Files.write(logFile.toPath(), Arrays.asList("{\"a\": 1}", "[{\"b\": 2}]"), StandardCharsets.UTF_8);
    try {
      long start = System.currentTimeMillis();
      StageRunner.Output output = runner.runProduce(null, 1000);
      long end = System.currentTimeMillis();
      Assert.assertTrue(end - start >= 1000);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Record record = output.getRecords().get("lane").get(0);
      Assert.assertEquals(1, record.get("/a").getValue());
      record = output.getRecords().get("lane").get(1);
      Assert.assertEquals(2, record.get("[0]/b").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  private final static String LINE1 = "2015-03-20 15:53:31,161 DEBUG PipelineConfigurationValidator - " +
      "Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=0";
  private final static String LINE2 = "2015-03-21 15:53:31,161 DEBUG PipelineConfigurationValidator - " +
      "Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=1";
  public static final String DATE_LEVEL_CLASS =
      "2015-03-24 17:49:16,808 ERROR ExceptionToHttpErrorProvider - ";

  public static final String ERROR_MSG_WITH_STACK_TRACE = "REST API call error: LOG_PARSER_01 - Error parsing log line '2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties', reason : 'LOG_PARSER_03 - Log line 2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties does not confirm to Log4j Log Format'\n" +
      "com.streamsets.pipeline.lib.parser.DataParserException: LOG_PARSER_01 - Error parsing log line '2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties', reason : 'LOG_PARSER_03 - Log line 2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties does not confirm to Log4j Log Format'\n" +
      "\tat com.streamsets.pipeline.lib.parser.log.LogDataParser.parse(LogDataParser.java:69)\n" +
      "\tat com.streamsets.pipeline.stage.origin.spooldir.SpoolDirSource.produce(SpoolDirSource.java:566)\n" +
      "\tat com.streamsets.pipeline.stage.origin.spooldir.SpoolDirSource.produce(SpoolDirSource.java:535)\n" +
      "\tat com.streamsets.pipeline.configurablestage.DSource.produce(DSource.java:24)\n" +
      "\tat com.streamsets.pipeline.runner.StageRuntime.execute(StageRuntime.java:149)\n" +
      "\tat com.streamsets.pipeline.runner.StagePipe.process(StagePipe.java:106)\n" +
      "\tat com.streamsets.pipeline.runner.preview.PreviewPipelineRunner.run(PreviewPipelineRunner.java:85)\n" +
      "\tat com.streamsets.pipeline.runner.Pipeline.run(Pipeline.java:98)\n" +
      "\tat com.streamsets.pipeline.runner.preview.PreviewPipeline.run(PreviewPipeline.java:38)\n" +
      "\tat com.streamsets.pipeline.restapi.PreviewResource.previewWithOverride(PreviewResource.java:105)\n" +
      "\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n" +
      "\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)\n" +
      "\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n" +
      "\tat java.lang.reflect.Method.invoke(Method.java:606)\n" +
      "\tat org.glassfish.jersey.server.model.internal.ResourceMethodInvocationHandlerFactory$1.invoke(ResourceMethodInvocationHandlerFactory.java:81)\n" +
      "\tat org.glassfish.jersey.server.model.internal.AbstractJavaResourceMethodDispatcher$1.run(AbstractJavaResourceMethodDispatcher.java:151)\n" +
      "\tat org.glassfish.jersey.server.model.internal.AbstractJavaResourceMethodDispatcher.invoke(AbstractJavaResourceMethodDispatcher.java:171)\n" +
      "\tat org.glassfish.jersey.server.model.internal.JavaResourceMethodDispatcherProvider$ResponseOutInvoker.doDispatch(JavaResourceMethodDispatcherProvider.java:152)\n" +
      "\tat org.glassfish.jersey.server.model.internal.AbstractJavaResourceMethodDispatcher.dispatch(AbstractJavaResourceMethodDispatcher.java:104)\n" +
      "\tat org.glassfish.jersey.server.model.ResourceMethodInvoker.invoke(ResourceMethodInvoker.java:384)\n" +
      "\tat org.glassfish.jersey.server.model.ResourceMethodInvoker.apply(ResourceMethodInvoker.java:342)\n" +
      "\tat org.glassfish.jersey.server.model.ResourceMethodInvoker.apply(ResourceMethodInvoker.java:101)\n" +
      "\tat org.glassfish.jersey.server.ServerRuntime$1.run(ServerRuntime.java:271)\n" +
      "\tat org.glassfish.jersey.internal.Errors$1.call(Errors.java:271)\n" +
      "\tat org.glassfish.jersey.internal.Errors$1.call(Errors.java:267)\n" +
      "\tat org.glassfish.jersey.internal.Errors.process(Errors.java:315)\n" +
      "\tat org.glassfish.jersey.internal.Errors.process(Errors.java:297)\n" +
      "\tat org.glassfish.jersey.internal.Errors.process(Errors.java:267)\n" +
      "\tat org.glassfish.jersey.process.internal.RequestScope.runInScope(RequestScope.java:297)\n" +
      "\tat org.glassfish.jersey.server.ServerRuntime.process(ServerRuntime.java:254)\n" +
      "\tat org.glassfish.jersey.server.ApplicationHandler.handle(ApplicationHandler.java:1030)\n" +
      "\tat org.glassfish.jersey.servlet.WebComponent.service(WebComponent.java:373)\n" +
      "\tat org.glassfish.jersey.servlet.ServletContainer.service(ServletContainer.java:381)\n" +
      "\tat org.glassfish.jersey.servlet.ServletContainer.service(ServletContainer.java:344)\n" +
      "\tat org.glassfish.jersey.servlet.ServletContainer.service(ServletContainer.java:221)\n" +
      "\tat org.eclipse.jetty.servlet.ServletHolder.handle(ServletHolder.java:769)\n" +
      "\tat org.eclipse.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1667)\n" +
      "\tat com.streamsets.pipeline.http.LocaleDetectorFilter.doFilter(LocaleDetectorFilter.java:29)\n" +
      "\tat org.eclipse.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1650)\n" +
      "\tat org.eclipse.jetty.servlets.UserAgentFilter.doFilter(UserAgentFilter.java:83)\n" +
      "\tat org.eclipse.jetty.servlets.GzipFilter.doFilter(GzipFilter.java:300)\n" +
      "\tat org.eclipse.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1650)\n" +
      "\tat org.eclipse.jetty.servlet.ServletHandler.doHandle(ServletHandler.java:583)\n" +
      "\tat org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:143)\n" +
      "\tat org.eclipse.jetty.security.SecurityHandler.handle(SecurityHandler.java:542)\n" +
      "\tat org.eclipse.jetty.server.session.SessionHandler.doHandle(SessionHandler.java:223)\n" +
      "\tat org.eclipse.jetty.server.handler.ContextHandler.doHandle(ContextHandler.java:1125)\n" +
      "\tat org.eclipse.jetty.servlet.ServletHandler.doScope(ServletHandler.java:515)\n" +
      "\tat org.eclipse.jetty.server.session.SessionHandler.doScope(SessionHandler.java:185)\n" +
      "\tat org.eclipse.jetty.server.handler.ContextHandler.doScope(ContextHandler.java:1059)\n" +
      "\tat org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:141)\n" +
      "\tat org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:97)\n" +
      "\tat org.eclipse.jetty.rewrite.handler.RewriteHandler.handle(RewriteHandler.java:309)\n" +
      "\tat org.eclipse.jetty.server.handler.HandlerCollection.handle(HandlerCollection.java:110)\n" +
      "\tat org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:97)\n" +
      "\tat org.eclipse.jetty.server.Server.handle(Server.java:497)\n" +
      "\tat org.eclipse.jetty.server.HttpChannel.handle(HttpChannel.java:311)\n" +
      "\tat org.eclipse.jetty.server.HttpConnection.onFillable(HttpConnection.java:248)\n" +
      "\tat org.eclipse.jetty.io.AbstractConnection$2.run(AbstractConnection.java:540)\n" +
      "\tat org.eclipse.jetty.util.thread.QueuedThreadPool.runJob(QueuedThreadPool.java:610)\n" +
      "\tat org.eclipse.jetty.util.thread.QueuedThreadPool$3.run(QueuedThreadPool.java:539)\n" +
      "\tat java.lang.Thread.run(Thread.java:745)\n" +
      "Caused by: com.streamsets.pipeline.lib.parser.DataParserException: LOG_PARSER_03 - Log line 2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties does not confirm to Log4j Log Format\n" +
      "\tat com.streamsets.pipeline.lib.parser.log.Log4jParser.handleNoMatch(Log4jParser.java:30)\n" +
      "\tat com.streamsets.pipeline.lib.parser.log.GrokParser.parseLogLine(GrokParser.java:51)\n" +
      "\tat com.streamsets.pipeline.lib.parser.log.LogDataParser.parse(LogDataParser.java:67)\n" +
      "\t... 61 more";

  public static final String LOG_LINE_WITH_STACK_TRACE = DATE_LEVEL_CLASS + ERROR_MSG_WITH_STACK_TRACE;


  @Test
  public void testTailLogFormat() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    File logFile = new File(testDataDir, "logFile.txt");

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/logFile.txt";
    fileInfo.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.LOG;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.logMaxObjectLen = 1024;
    conf.dataFormatConfig.logMode = LogMode.LOG4J;
    conf.dataFormatConfig.retainOriginalLine = true;
    conf.dataFormatConfig.enableLog4jCustomLogFormat = false;
    conf.dataFormatConfig.log4jCustomLogFormat = null;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane").addOutputLane("metadata")
        .build();
    runner.runInit();
    Files.write(logFile.toPath(), Arrays.asList(LINE1, LINE2), StandardCharsets.UTF_8);
    try {
      long start = System.currentTimeMillis();
      StageRunner.Output output = runner.runProduce(null, 10);
      long end = System.currentTimeMillis();
      Assert.assertTrue(end - start >= 1000);
      Assert.assertNotNull(output.getNewOffset());
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(2, records.size());
      Assert.assertFalse(records.get(0).has("/truncated"));

      Record record = records.get(0);

      Assert.assertEquals(LINE1, record.get().getValueAsMap().get("originalLine").getValueAsString());

      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
      Assert.assertEquals("2015-03-20 15:53:31,161", record.get("/" + Constants.TIMESTAMP).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.SEVERITY));
      Assert.assertEquals("DEBUG", record.get("/" + Constants.SEVERITY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.CATEGORY));
      Assert.assertEquals("PipelineConfigurationValidator", record.get("/" + Constants.CATEGORY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.MESSAGE));
      Assert.assertEquals("Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=0",
          record.get("/" + Constants.MESSAGE).getValueAsString());

      record = records.get(1);

      Assert.assertEquals(LINE2, record.get().getValueAsMap().get("originalLine").getValueAsString());
      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
      Assert.assertEquals("2015-03-21 15:53:31,161", record.get("/" + Constants.TIMESTAMP).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.SEVERITY));
      Assert.assertEquals("DEBUG", record.get("/" + Constants.SEVERITY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.CATEGORY));
      Assert.assertEquals("PipelineConfigurationValidator", record.get("/" + Constants.CATEGORY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.MESSAGE));
      Assert.assertEquals("Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=1",
          record.get("/" + Constants.MESSAGE).getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTailLogFormatStackTrace() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    File logFile = new File(testDataDir, "logFile.txt");

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/logFile.txt";
    fileInfo.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "^[0-9].*";
    conf.batchSize = 2;
    conf.maxWaitTimeSecs = 1000;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 2048;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane").addOutputLane("metadata")
        .build();
    runner.runInit();
    Files.write(logFile.toPath(), Arrays.asList(LINE1, LOG_LINE_WITH_STACK_TRACE, LINE2), StandardCharsets.UTF_8);
    try {
      StageRunner.Output out = runner.runProduce(null, 100);
      Assert.assertEquals(2, out.getRecords().get("lane").size());
      Assert.assertEquals(LINE1, out.getRecords().get("lane").get(0).get("/text").getValueAsString().trim());
      Assert.assertEquals(LOG_LINE_WITH_STACK_TRACE, out.getRecords().get("lane").get(1).get("/text").getValueAsString().trim());
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testTailFilesDeletion() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    File testDataDir1 = new File(testDataDir, UUID.randomUUID().toString());
    File testDataDir2 = new File(testDataDir, UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir1.mkdirs());
    Assert.assertTrue(testDataDir2.mkdirs());
    Path file1 = new File(testDataDir1, "log1.txt").toPath();
    Path file2 = new File(testDataDir2, "log2.txt").toPath();
    Files.write(file1, Arrays.asList("Hello"), UTF8);
    Files.write(file2, Arrays.asList("Hola"), UTF8);

    FileInfo fileInfo1 = new FileInfo();
    fileInfo1.fileFullPath = testDataDir.getAbsolutePath() + "/*/log*.txt";
    fileInfo1.fileRollMode = FileRollMode.REVERSE_COUNTER;
    fileInfo1.firstFile = "";
    fileInfo1.patternForToken = "";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo1);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane").addOutputLane("metadata")
        .build();
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProduce(null, 10);
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertTrue(output.getNewOffset().contains("log1.txt"));
      Assert.assertTrue(output.getNewOffset().contains("log2.txt"));
      Files.delete(file1);
      Files.delete(testDataDir1.toPath());
      output = runner.runProduce(output.getNewOffset(), 10);
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertFalse(output.getNewOffset().contains("log1.txt"));
      Assert.assertTrue(output.getNewOffset().contains("log2.txt"));
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testPeriodicFiles() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    String logFile = new File(testDataDir, "logFile.txt-1").getAbsolutePath();
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("testLogFile.txt");
    OutputStream os = new FileOutputStream(logFile);
    IOUtils.copy(is, os);
    is.close();
    os.close();

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/logFile.txt-${PATTERN}";
    fileInfo.fileRollMode = FileRollMode.PATTERN;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "[0-9]";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane").addOutputLane("metadata")
        .build();
    runner.runInit();
    try {
      long start = System.currentTimeMillis();
      StageRunner.Output output = runner.runProduce(null, 1000);
      long end = System.currentTimeMillis();
      Assert.assertTrue(end - start >= 1000);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Record record = output.getRecords().get("lane").get(0);
      Assert.assertEquals("FIRST", record.get("/text").getValueAsString());
      record = output.getRecords().get("lane").get(1);
      Assert.assertEquals("LAST", record.get("/text").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  private Source createSourceForPeriodicFile(String filePathWithPattern, String pattern) {
    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = filePathWithPattern;
    fileInfo.fileRollMode = FileRollMode.PATTERN;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = pattern;

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    return new FileTailSource(conf, SCAN_INTERVAL);
  }

  private SourceRunner createRunner(Source source) {
    return new SourceRunner.Builder(FileTailDSource.class, source).addOutputLane("lane").addOutputLane("meta").build();
  }

  @Test
  public void testFileTruncatedBetweenRuns() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    File file = new File(testDataDir, "file.txt-1");
    Files.write(file.toPath(), Arrays.asList("A", "B", "C"), StandardCharsets.UTF_8);

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}";
    fileInfo.fileRollMode = FileRollMode.PATTERN;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "[0-9]";
    Source source = createSourceForPeriodicFile(testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}", "[0-9]");
    SourceRunner runner = createRunner(source);
    try {
      // run till current end and stop pipeline
      runner.runInit();
      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertEquals(3, output.getRecords().get("lane").size());
      runner.runDestroy();

      // truncate file
      FileChannel channel = new FileOutputStream(file, true).getChannel();
      channel.truncate(2);
      channel.close();

      // run again, no new data, no error
      source = createSourceForPeriodicFile(testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}", "[0-9]");
      runner = createRunner(source);
      runner.runInit();
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(0, output.getRecords().get("lane").size());
      runner.runDestroy();

      file = new File(testDataDir, "file.txt-2");
      Files.write(file.toPath(), Arrays.asList("A", "B"), StandardCharsets.UTF_8);

      // run again, new file
      source = createSourceForPeriodicFile(testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}", "[0-9]");
      runner = createRunner(source);
      runner.runInit();
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(2, output.getRecords().get("lane").size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFileDeletedBetweenRuns() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    File file = new File(testDataDir, "file.txt-1");
    Files.write(file.toPath(), Arrays.asList("A", "B", "C"), StandardCharsets.UTF_8);

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}";
    fileInfo.fileRollMode = FileRollMode.PATTERN;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "[0-9]";
    Source source = createSourceForPeriodicFile(testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}", "[0-9]");
    SourceRunner runner = createRunner(source);
    try {
      // run till current end and stop pipeline
      runner.runInit();
      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertEquals(3, output.getRecords().get("lane").size());
      runner.runDestroy();

      Files.delete(file.toPath());

      // run again, no new data, no error
      source = createSourceForPeriodicFile(testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}", "[0-9]");
      runner = createRunner(source);
      runner.runInit();
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(0, output.getRecords().get("lane").size());
      runner.runDestroy();

      file = new File(testDataDir, "file.txt-2");
      Files.write(file.toPath(), Arrays.asList("A", "B"), StandardCharsets.UTF_8);

      // run again, new file
      source = createSourceForPeriodicFile(testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}", "[0-9]");
      runner = createRunner(source);
      runner.runInit();
      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(2, output.getRecords().get("lane").size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFileDeletedWhileRunning() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    File file = new File(testDataDir, "file.txt-1");
    Files.write(file.toPath(), Arrays.asList("A", "B", "C"), StandardCharsets.UTF_8);

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}";
    fileInfo.fileRollMode = FileRollMode.PATTERN;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "[0-9]";
    Source source = createSourceForPeriodicFile(testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}", "[0-9]");
    SourceRunner runner = createRunner(source);
    try {
      // run till current end and stop pipeline
      runner.runInit();
      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertEquals(3, output.getRecords().get("lane").size());

      Files.delete(file.toPath());

      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(0, output.getRecords().get("lane").size());

      file = new File(testDataDir, "file.txt-2");
      Files.write(file.toPath(), Arrays.asList("A", "B"), StandardCharsets.UTF_8);

      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(2, output.getRecords().get("lane").size());

    } finally {
      runner.runDestroy();
    }
  }

  private void writeFileInDirectoryStructure(File baseDir, String suffixDirPath, int times) throws IOException {
    for (int i = 1; i <= times; i++) {
      //Create ${baseDir}/${uuid}/dir/indx_${i}/${suffixDir}/file.txt-1
      File fullTestIndxDirPath = new File(baseDir.getAbsolutePath() + "/dir/indx_" + i + "/" + suffixDirPath);
      Assert.assertTrue(
          Utils.format("Unable to create test folder :{}", fullTestIndxDirPath.getAbsolutePath()),
          fullTestIndxDirPath.mkdirs()
      );
      File file = new File(fullTestIndxDirPath, "file.txt-1");
      Files.write(file.toPath(), Arrays.asList("A", "B", "C"), StandardCharsets.UTF_8);
    }
  }

  @Test
  public void testCreationOfLateDirectories() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());

    String suffixDirPath = "data";

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/dir/*/" + suffixDirPath + "/file.txt-1";
    fileInfo.fileRollMode = FileRollMode.ALPHABETICAL;
    fileInfo.firstFile = "";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 1024;
    conf.allowLateDirectories = true;


    Source source = new FileTailSource(conf, 1);

    SourceRunner runner = createRunner(source);
    try {
      // run till current end and stop pipeline
      runner.runInit();
      StageRunner.Output output = runner.runProduce(null, 10);

      Assert.assertEquals(0, output.getRecords().get("lane").size());

      writeFileInDirectoryStructure(testDataDir, suffixDirPath, 3);

      //Give about 10 secs for the directory findDirectoryPathCreationWatcher and FileFinder thread to detect the file appearance.
      Thread.sleep(10000);

      output = runner.runProduce(output.getNewOffset(), 10);
      Assert.assertEquals(3 * 3, output.getRecords().get("lane").size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testOffsetLagMetric() throws Exception {
    final File testDataDir = new File("target", UUID.randomUUID().toString());

    Assert.assertTrue(testDataDir.mkdirs());

    final File file = new File(testDataDir, "file.txt-1");

    Files.write(file.toPath(), Arrays.asList("A", "B", "C"), StandardCharsets.UTF_8);

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/file.txt-1";
    fileInfo.fileRollMode = FileRollMode.ALPHABETICAL;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = ".*";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    FileTailSource source = PowerMockito.spy(new FileTailSource(conf, SCAN_INTERVAL));

    //Intercept getOffsetsLag private method which calculates the offsetLag
    //in the files and write some data to file so there is an offset lag.
    PowerMockito.replace(
        MemberMatcher.method(
            FileTailSource.class,
            "calculateOffsetLagMetric",
            Map.class
        )
    ).with(
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            //This will add 6 more (D, E, F) to the file and move the file size to 12 bytes
            //but we would have read just 6 bytes (A, B, C).
            Files.write(
                file.toPath(),
                Arrays.asList("D", "E", "F"),
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND
            );
            //call the real getOffsetsLag private method
            return method.invoke(proxy, args);
          }
        }
    );

    SourceRunner runner = createRunner(source);
    try {
      runner.runInit();

      StageRunner.Output output = runner.runProduce(null, 10);

      // Make sure there are only three records.
      Assert.assertEquals(3, output.getRecords().get("lane").size());

      Map<String, Counter> offsetLag = (Map<String, Counter>) Whitebox.getInternalState(source, "offsetLagMetric");
      Assert.assertEquals(6L, offsetLag.get(file.getAbsolutePath() + "||" + ".*").getCount());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testPendingFilesMetric() throws Exception {
    final File testDataDir = new File("target", UUID.randomUUID().toString());

    Assert.assertTrue(testDataDir.mkdirs());

    final List<File> files = Arrays.asList(
        new File(testDataDir, "file.txt-1"),
        new File(testDataDir, "file.txt-2"),
        new File(testDataDir, "file.txt-3"),
        new File(testDataDir, "file.txt-4"),
        new File(testDataDir, "file.txt-5"),
        new File(testDataDir, "file.txt-6"),
        new File(testDataDir, "file.txt-7"),
        new File(testDataDir, "file.txt-8")
    );

    //We will create first 4 files here. Rest of the four files will be created
    //before we calculate the pending files metric.
    for (int i = 0; i < 4; i++) {
      File file = files.get(i);
      Files.write(
          file.toPath(),
          Arrays.asList("A", "B", "C"),
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE_NEW
      );
    }

    FileTailSource source = PowerMockito.spy(
        (FileTailSource) createSourceForPeriodicFile(
            testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}",
            "[0-9]"
        )
    );

    //Intercept calculatePendingFilesMetric private method which calculates the pendingFiles
    //and create new files.
    PowerMockito.replace(
        MemberMatcher.method(
            FileTailSource.class,
            "calculatePendingFilesMetric"
        )
    ).with(
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            //Create the remaining 4 files so as to have files which are pending and not being started for processing.
            for (int i = 4; i < 8; i++) {
              File file = files.get(i);
              Files.write(
                  file.toPath(),
                  Arrays.asList("A", "B", "C"),
                  StandardCharsets.UTF_8,
                  StandardOpenOption.CREATE_NEW
              );
            }
            //call the real getOffsetsLag private method
            return method.invoke(proxy, args);
          }
        }
    );

    SourceRunner runner = createRunner(source);
    try {
      runner.runInit();

      StageRunner.Output output = runner.runProduce(null, 36);

      // Make sure there are only 12 (3 for each file we read).
      Assert.assertEquals(12, output.getRecords().get("lane").size());

      Map<String, Counter> pendingFilesMetric =
          (Map<String, Counter>) Whitebox.getInternalState(
              source,
              "pendingFilesMetric"
          );
      Assert.assertEquals(
          4L,
          pendingFilesMetric.get(
              testDataDir.getAbsolutePath() + "/file.txt-${PATTERN}||[0-9]"
          ).getCount()
      );

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testGlobbingForPeriodicRollPattern() throws Exception {
    final File testDataDir = new File("target", UUID.randomUUID().toString());

    Assert.assertTrue(testDataDir.mkdirs());

    final List<Path> dirPaths = Arrays.asList(
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a1" + File.separatorChar + "b1"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a2" + File.separatorChar + "b2"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a3" + File.separatorChar + "b3"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a4" + File.separatorChar + "b4"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a5" + File.separatorChar + "b5"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a6" + File.separatorChar + "b6")
    );

    for (int i = 0; i< dirPaths.size(); i++) {
      Path dirPath = dirPaths.get(i);
      Files.createDirectories(dirPath);
      Path filePath1 = Paths.get(dirPath.toString() + File.separatorChar + "file-"+ String.valueOf(i) +".txt");
      Files.write(
          filePath1,
          Arrays.asList("A", "B", "C"),
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE_NEW
      );
    }

    FileInfo fileInfo = new FileInfo();
    //We are looking for testdataDir/*/*/file-[0-9]+.txt
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + File.separatorChar + "*" +
        File.separatorChar + "*" + File.separatorChar + "file-${PATTERN}.txt";
    fileInfo.fileRollMode = FileRollMode.PATTERN;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "[0-9]+";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;

    conf.dataFormatConfig.textMaxLineLen = 1024;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);

    SourceRunner runner = createRunner(source);
    try {
      // run till current end and stop pipeline
      runner.runInit();
      StageRunner.Output output = runner.runProduce(null, 30);
      // (6 folders * 1 file * 3 records) = 18 records
      Assert.assertEquals(18L, output.getRecords().get("lane").size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMetricsWithGlobbingAndLateDirectory() throws Exception{
    final File testDataDir = new File("target", UUID.randomUUID().toString());

    final List<Path> dirPaths = Arrays.asList(
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a1" +
            File.separatorChar + "const" + File.separatorChar + "b1"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a2" +
            File.separatorChar + "const" + File.separatorChar + "b2"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a3" +
            File.separatorChar + "const" + File.separatorChar + "b3"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a4" +
            File.separatorChar + "const" + File.separatorChar + "b4"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a5" +
            File.separatorChar + "const" + File.separatorChar + "b5"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a6" +
            File.separatorChar + "const" + File.separatorChar + "b6"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a7" +
            File.separatorChar + "const" + File.separatorChar + "b7"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a8" +
            File.separatorChar + "const" + File.separatorChar + "b8"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a9" +
            File.separatorChar + "const" + File.separatorChar + "b9"),
        Paths.get(testDataDir.getAbsolutePath() + File.separatorChar + "a10" +
            File.separatorChar + "const" + File.separatorChar + "b10")
    );

    FileInfo fileInfo = new FileInfo();
    //We are looking for testdataDir/*/const/*/file-[0-9]+.txt
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + File.separatorChar + "*" +
        File.separatorChar + "const" + File.separatorChar + "*" + File.separatorChar +
        "file-${PATTERN}.txt";
    fileInfo.fileRollMode = FileRollMode.PATTERN;
    fileInfo.firstFile = "";
    fileInfo.patternForToken = "[0-9]+";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 40;
    conf.maxWaitTimeSecs = 40;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.allowLateDirectories = true;

    conf.dataFormatConfig.textMaxLineLen = 1024;

    FileTailSource source = PowerMockito.spy(new FileTailSource(conf, SCAN_INTERVAL));

    SourceRunner runner = createRunner(source);

    // run till current end and stop pipeline
    runner.runInit();

    //Late directory(testDataDir) creation
    Assert.assertTrue(testDataDir.mkdirs());

    for (int i=0; i<dirPaths.size(); i++) {
      Path dirPath = dirPaths.get(i);
      Files.createDirectories(dirPath);
      Path filePath = Paths.get(dirPath.toString() + File.separatorChar + "file-"+ String.valueOf(i) +".txt");
      Files.write(
          filePath,
          Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"), //10 records * 2 bytes = 20 bytes.
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE_NEW
      );
    }

    setSleepTimeForFindingPaths(10000L , 2000L);

    try {
      //We have totally 10(folders) * 1(file) * 10 (records) = 100 records
      //Also means totally 100 records * 2 bytes = 200 bytes.
      //We will read only 20 records.
      //This means the total is (remaining 80 records * 2 bytes) = 160 bytes yet to be read.
      //including pending files and offsetLag.
      StageRunner.Output output = runner.runProduce(null, 20);
      Assert.assertEquals(20L, output.getRecords().get("lane").size());
      checkPendingAndOffsetLag(source, 160L);

      //All files would have been found, don't need to wait for finding them.
      setSleepTimeForFindingPaths(0L, 0L);

      //We are gonna read another 40 records and check
      //If we read 40 more records this means, we will end up reading another 40*2 = 80 bytes
      //Total remaining bytes to read is 160 - 80 = 80 bytes
      output = runner.runProduce(output.getNewOffset(), 40);
      Assert.assertEquals(40L, output.getRecords().get("lane").size());
      checkPendingAndOffsetLag(source, 80L);

      //We are gonna read 40 records and check
      //If we read 40 more records this means, we will end up reading another 40*2 = 80 bytes
      //Total remaining bytes to read is 80 - 80 = 0 bytes.
      output = runner.runProduce(output.getNewOffset(), 40);
      Assert.assertEquals(40L, output.getRecords().get("lane").size());
      checkPendingAndOffsetLag(source, 0L);
    } finally {
      runner.runDestroy();
    }
  }

  private void setSleepTimeForFindingPaths(final long dirFindTime, final long fileFindTime) {
    PowerMockito.replace(
        MemberMatcher.method(
            GlobFileContextProvider.class,
            "findCreatedDirectories"
        )
    ).with(
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            //Give some time for it to find new directories.
            Thread.sleep(dirFindTime);
            //call the real getOffsetsLag private method
            return method.invoke(proxy, args);
          }
        }
    );
    PowerMockito.replace(
        MemberMatcher.method(
            GlobFileContextProvider.class,
            "findNewFileContexts"
        )
    ).with(
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            //Give some time for it to find new files.
            Thread.sleep(fileFindTime);
            //call the real getOffsetsLag private method
            return method.invoke(proxy, args);
          }
        }
    );
  }

  @SuppressWarnings("unchecked")
  private void checkPendingAndOffsetLag(FileTailSource source, long expectedRemainingBytesToRead) {
    long totalOffsetLag = 0, totalBytesForPendingFiles = 0;
    Map<String, Counter> offsetLagMetric = Whitebox.getInternalState(source, "offsetLagMetric");
    Map<String, Counter> pendingFilesMetric = Whitebox.getInternalState(source, "pendingFilesMetric");

    for (Map.Entry<String, Counter> offsetLagMetricEntry : offsetLagMetric.entrySet()) {
      LOGGER.info("File Configuration :" + offsetLagMetricEntry.getKey()
          + " lags by " + offsetLagMetricEntry.getValue().getCount());
      totalOffsetLag += offsetLagMetricEntry.getValue().getCount();
    }

    LOGGER.info("Total Lag :" + totalOffsetLag);

    for (Map.Entry<String, Counter> pendingFilesMetricEntry : pendingFilesMetric.entrySet()) {
      LOGGER.info(
          "File Configuration :" + pendingFilesMetricEntry.getKey()
              + " has Pending Files: " + pendingFilesMetricEntry.getValue().getCount()
      );
      //Each pending file contributes - 10 records * 2 bytes = 20 bytes each.
      totalBytesForPendingFiles += (pendingFilesMetricEntry.getValue().getCount() * 10 * 2);
    }

    LOGGER.info("Total Pending Files Lag :" + totalBytesForPendingFiles);

    Assert.assertEquals(expectedRemainingBytesToRead, totalOffsetLag + totalBytesForPendingFiles);
  }

  // SDC-5457
  @Test
  public void testInitialFileAndPatterMode() throws Exception {
    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = "/random/path";
    fileInfo.fileRollMode = FileRollMode.PATTERN;
    fileInfo.firstFile = "the-best-filename-ever.txt";
    fileInfo.patternForToken = ".*";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
      .addOutputLane("lane")
      .addOutputLane("metadata")
      .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    String issue = issues.get(0).toString();
    Assert.assertTrue(issue, issue.contains("TAIL_08"));
  }

  // SDC-7598
  @Test
  public void testInitialFile() throws Exception {
    final File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());

    ImmutableList.of("server.log", "server.log.20171013", "server.log.20171014", "server.log.20171015").forEach(name -> {
      try {
        Path path = Paths.get(testDataDir.getAbsolutePath(), name);
        Files.write(path, name.getBytes(), StandardOpenOption.CREATE_NEW);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    FileInfo fileInfo = new FileInfo();
    fileInfo.fileFullPath = testDataDir.getAbsolutePath() + "/server.log";
    fileInfo.fileRollMode = FileRollMode.ALPHABETICAL;
    fileInfo.firstFile = "server.log.20171014";

    FileTailConfigBean conf = new FileTailConfigBean();
    conf.dataFormat = DataFormat.TEXT;
    conf.multiLineMainPattern = "";
    conf.batchSize = 25;
    conf.maxWaitTimeSecs = 1;
    conf.fileInfos = Arrays.asList(fileInfo);
    conf.postProcessing = PostProcessingOptions.NONE;
    conf.dataFormatConfig.textMaxLineLen = 1024;

    FileTailSource source = new FileTailSource(conf, SCAN_INTERVAL);

    SourceRunner runner = createRunner(source);
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProduce(null, 30);
      List<Record> records;

      // Normal output records
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(2, records.size());
      Assert.assertEquals("server.log.20171014", records.get(0).get("/text").getValueAsString());
      Assert.assertEquals("server.log.20171015", records.get(1).get("/text").getValueAsString());

      // Metadata output records
      records = output.getRecords().get("meta");
      Assert.assertNotNull(records);
      Assert.assertEquals(5, records.size());
      Assert.assertEquals("START", records.get(0).get("/event").getValueAsString());
      Assert.assertTrue(records.get(0).get("/fileName").getValueAsString().endsWith("server.log.20171014"));
      Assert.assertEquals("END", records.get(1).get("/event").getValueAsString());
      Assert.assertTrue(records.get(1).get("/fileName").getValueAsString().endsWith("server.log.20171014"));
      Assert.assertEquals("START", records.get(2).get("/event").getValueAsString());
      Assert.assertTrue(records.get(2).get("/fileName").getValueAsString().endsWith("server.log.20171015"));
      Assert.assertEquals("END", records.get(3).get("/event").getValueAsString());
      Assert.assertTrue(records.get(3).get("/fileName").getValueAsString().endsWith("server.log.20171015"));
      Assert.assertEquals("START", records.get(4).get("/event").getValueAsString());
      Assert.assertTrue(records.get(4).get("/fileName").getValueAsString().endsWith("server.log"));
    } finally {
      runner.runDestroy();
    }
  }
}
