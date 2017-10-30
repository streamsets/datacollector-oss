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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirRunnable;
import com.streamsets.pipeline.lib.parser.log.Constants;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestLogSpoolDirSourceLog4jFormat {
  private static final int threadNumber = 0;
  private static final int batchSize = 10;
  private static final Map<String, Offset> lastSourceOffset = new HashMap<>();

  private String createTestDir() {
    File f = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(f.mkdirs());
    return f.getAbsolutePath();
  }

  private final static String LINE1 = "2015-03-20 15:53:31,161 DEBUG PipelineConfigurationValidator - " +
    "Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=0";
  private final static String LINE2 = "2015-03-21 15:53:31,161 DEBUG PipelineConfigurationValidator - " +
    "Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=1";

  private final static String TTCC_LINE1 =  "176 [main] INFO  org.apache.log4j.examples.Sort - Populating an array of 1 elements in reverse order.";
  private final static String TTCC_LINE2 =  "177 [main] WARN  org.apache.log4j.examples.Sort - Populating an array of 2 elements in reverse order.";

  private static final String DATE_LEVEL_CLASS =
    "2015-03-24 17:49:16,808 ERROR ExceptionToHttpErrorProvider - ";

  private static final String ERROR_MSG_WITH_STACK_TRACE = "REST API call error: LOG_PARSER_01 - Error parsing log line '2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties', reason : 'LOG_PARSER_03 - Log line 2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties does not confirm to Log4j Log Format'\n" +
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

  private static final String LOG_LINE_WITH_STACK_TRACE = DATE_LEVEL_CLASS + ERROR_MSG_WITH_STACK_TRACE;

  private File createLogFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    IOUtils.write(LINE1 + "\n", writer);
    IOUtils.write(LINE2, writer);
    writer.close();
    return f;
  }

  private File createLogFileWithStackTrace() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    IOUtils.write(LINE1 + "\n", writer);
    IOUtils.write(LOG_LINE_WITH_STACK_TRACE + "\n", writer);
    IOUtils.write(LINE2, writer);
    writer.close();
    return f;
  }

  private File createTTCCLogFile() throws Exception {
    File f = new File(createTestDir(), "test.log");
    Writer writer = new FileWriter(f);
    IOUtils.write(TTCC_LINE1 + "\n", writer);
    IOUtils.write(TTCC_LINE2, writer);
    writer.close();
    return f;
  }

  private SpoolDirSource createSource(OnParseError onParseError, int maxStackTraceLines) {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.LOG;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.overrunLimit = 100;
    conf.spoolDir = createTestDir();
    conf.batchSize = 10;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "file-[0-9].log";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.ARCHIVE;
    conf.archiveDir = createTestDir();
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.logMode = LogMode.LOG4J;
    conf.dataFormatConfig.logMaxObjectLen = 10000;
    conf.dataFormatConfig.retainOriginalLine = true;
    conf.dataFormatConfig.enableLog4jCustomLogFormat = false;
    conf.dataFormatConfig.log4jCustomLogFormat = null;
    conf.dataFormatConfig.onParseError = onParseError;
    conf.dataFormatConfig.maxStackTraceLines = maxStackTraceLines;

    return new SpoolDirSource(conf);
  }

  private SpoolDirSource createSource() {
    return createSource(OnParseError.INCLUDE_AS_STACK_TRACE, 150);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    SpoolDirSource source = createSource();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(createLogFile(), "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
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

      Assert.assertEquals(LINE2, records.get(1).get().getValueAsMap().get("originalLine").getValueAsString());
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
  public void testProduceLessThanFile() throws Exception {
    SpoolDirSource source = createSource();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      String offset = runnable.generateBatch(createLogFile(), "0", 1, batchMaker);
      Assert.assertEquals("142", offset);
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());


      Record record = records.get(0);
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

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = runnable.generateBatch(createLogFile(), offset, 1, batchMaker);
      Assert.assertEquals("283", offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(1, records.size());

      Assert.assertEquals(LINE2, records.get(0).get().getValueAsMap().get("originalLine").getValueAsString());
      Assert.assertFalse(records.get(0).has("/truncated"));

      record = records.get(0);

      Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
      Assert.assertEquals("2015-03-21 15:53:31,161", record.get("/" + Constants.TIMESTAMP).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.SEVERITY));
      Assert.assertEquals("DEBUG", record.get("/" + Constants.SEVERITY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.CATEGORY));
      Assert.assertEquals("PipelineConfigurationValidator", record.get("/" + Constants.CATEGORY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.MESSAGE));
      Assert.assertEquals("Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=1",
        record.get("/" + Constants.MESSAGE).getValueAsString());

      batchMaker = SourceRunner.createTestBatchMaker("lane");
      offset = runnable.generateBatch(createLogFile(), offset, 1, batchMaker);
      Assert.assertEquals("-1", offset);
      output = SourceRunner.getOutput(batchMaker);
      records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(0, records.size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceFullFileCustomLogFormat() throws Exception {
    SpoolDirConfigBean conf = new SpoolDirConfigBean();
    conf.dataFormat = DataFormat.LOG;
    conf.dataFormatConfig.charset = "UTF-8";
    conf.dataFormatConfig.removeCtrlChars = false;
    conf.overrunLimit = 100;
    conf.spoolDir = createTestDir();
    conf.batchSize = 10;
    conf.poolingTimeoutSecs = 1;
    conf.filePattern = "file-[0-9].log";
    conf.pathMatcherMode = PathMatcherMode.GLOB;
    conf.maxSpoolFiles = 10;
    conf.initialFileToProcess = null;
    conf.dataFormatConfig.compression = Compression.NONE;
    conf.dataFormatConfig.filePatternInArchive = "*";
    conf.errorArchiveDir = null;
    conf.postProcessing = PostProcessingOptions.ARCHIVE;
    conf.archiveDir = createTestDir();
    conf.retentionTimeMins = 10;
    conf.dataFormatConfig.logMode = LogMode.LOG4J;
    conf.dataFormatConfig.logMaxObjectLen = 1000;
    conf.dataFormatConfig.retainOriginalLine = true;
    conf.dataFormatConfig.enableLog4jCustomLogFormat = true;
    conf.dataFormatConfig.log4jCustomLogFormat = "%-6r [%15.15t] %-5p %30.30c - %m";
    conf.dataFormatConfig.onParseError = OnParseError.ERROR;
    conf.dataFormatConfig.maxStackTraceLines = 0;

    SpoolDirSource source = new SpoolDirSource(conf);
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(createTTCCLogFile(), "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(2, records.size());

      Assert.assertFalse(records.get(0).has("/truncated"));

      Record record = records.get(0);

      Assert.assertEquals(TTCC_LINE1, record.get().getValueAsMap().get("originalLine").getValueAsString());

      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/" + Constants.RELATIVETIME));
      Assert.assertEquals("176", record.get("/" + Constants.RELATIVETIME).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.THREAD));
      Assert.assertEquals("main", record.get("/" + Constants.THREAD).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.SEVERITY));
      Assert.assertEquals("INFO", record.get("/" + Constants.SEVERITY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.CATEGORY));
      Assert.assertEquals("org.apache.log4j.examples.Sort",
        record.get("/" + Constants.CATEGORY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.MESSAGE));
      Assert.assertEquals("Populating an array of 1 elements in reverse order.",
        record.get("/" + Constants.MESSAGE).getValueAsString());

     record = records.get(1);

      Assert.assertEquals(TTCC_LINE2, record.get().getValueAsMap().get("originalLine").getValueAsString());

      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/" + Constants.RELATIVETIME));
      Assert.assertEquals("177", record.get("/" + Constants.RELATIVETIME).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.THREAD));
      Assert.assertEquals("main", record.get("/" + Constants.THREAD).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.SEVERITY));
      Assert.assertEquals("WARN", record.get("/" + Constants.SEVERITY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.CATEGORY));
      Assert.assertEquals("org.apache.log4j.examples.Sort",
        record.get("/" + Constants.CATEGORY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.MESSAGE));
      Assert.assertEquals("Populating an array of 2 elements in reverse order.",
        record.get("/" + Constants.MESSAGE).getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testProduceFullFileWithStackTrace() throws Exception {
    SpoolDirSource source = createSource();
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(createLogFileWithStackTrace(), "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(3, records.size());

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

      Assert.assertEquals(LOG_LINE_WITH_STACK_TRACE, record.get().getValueAsMap().get("originalLine").getValueAsString());

      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
      Assert.assertEquals("2015-03-24 17:49:16,808", record.get("/" + Constants.TIMESTAMP).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.SEVERITY));
      Assert.assertEquals("ERROR", record.get("/" + Constants.SEVERITY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.CATEGORY));
      Assert.assertEquals("ExceptionToHttpErrorProvider", record.get("/" + Constants.CATEGORY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.MESSAGE));
      Assert.assertEquals(ERROR_MSG_WITH_STACK_TRACE, record.get("/" + Constants.MESSAGE).getValueAsString());

      record = records.get(2);

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
  public void testProduceFullFileWithStackTraceCutShort() throws Exception {
    SpoolDirSource source = createSource(OnParseError.INCLUDE_AS_STACK_TRACE, 10);
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      Assert.assertEquals("-1", runnable.generateBatch(createLogFileWithStackTrace(), "0", 10, batchMaker));
      StageRunner.Output output = SourceRunner.getOutput(batchMaker);
      List<Record> records = output.getRecords().get("lane");
      Assert.assertNotNull(records);
      Assert.assertEquals(3, records.size());

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

      Assert.assertEquals(DATE_LEVEL_CLASS +
          "REST API call error: LOG_PARSER_01 - Error parsing log line '2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties', reason : 'LOG_PARSER_03 - Log line 2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties does not confirm to Log4j Log Format'\n" +
          "com.streamsets.pipeline.lib.parser.DataParserException: LOG_PARSER_01 - Error parsing log line '2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties', reason : 'LOG_PARSER_03 - Log line 2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties does not confirm to Log4j Log Format'\n" +
          "\tat com.streamsets.pipeline.lib.parser.log.LogDataParser.parse(LogDataParser.java:69)\n" +
          "\tat com.streamsets.pipeline.stage.origin.spooldir.SpoolDirSource.produce(SpoolDirSource.java:566)\n" +
          "\tat com.streamsets.pipeline.stage.origin.spooldir.SpoolDirSource.produce(SpoolDirSource.java:535)\n" +
          "\tat com.streamsets.pipeline.configurablestage.DSource.produce(DSource.java:24)\n" +
          "\tat com.streamsets.pipeline.runner.StageRuntime.execute(StageRuntime.java:149)\n" +
          "\tat com.streamsets.pipeline.runner.StagePipe.process(StagePipe.java:106)\n" +
          "\tat com.streamsets.pipeline.runner.preview.PreviewPipelineRunner.run(PreviewPipelineRunner.java:85)\n" +
          "\tat com.streamsets.pipeline.runner.Pipeline.run(Pipeline.java:98)\n" +
          "\tat com.streamsets.pipeline.runner.preview.PreviewPipeline.run(PreviewPipeline.java:38)",
        record.get().getValueAsMap().get("originalLine").getValueAsString());

      Assert.assertFalse(record.has("/truncated"));

      Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
      Assert.assertEquals("2015-03-24 17:49:16,808", record.get("/" + Constants.TIMESTAMP).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.SEVERITY));
      Assert.assertEquals("ERROR", record.get("/" + Constants.SEVERITY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.CATEGORY));
      Assert.assertEquals("ExceptionToHttpErrorProvider", record.get("/" + Constants.CATEGORY).getValueAsString());

      Assert.assertTrue(record.has("/" + Constants.MESSAGE));
      Assert.assertEquals("REST API call error: LOG_PARSER_01 - Error parsing log line '2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties', reason : 'LOG_PARSER_03 - Log line 2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties does not confirm to Log4j Log Format'\n" +
          "com.streamsets.pipeline.lib.parser.DataParserException: LOG_PARSER_01 - Error parsing log line '2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties', reason : 'LOG_PARSER_03 - Log line 2015-03-24 12:38:05,206 DEBUG LogConfigurator - Log starting, from configuration: /Users/harikiran/Documents/workspace/streamsets/dev/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc/log4j.properties does not confirm to Log4j Log Format'\n" +
          "\tat com.streamsets.pipeline.lib.parser.log.LogDataParser.parse(LogDataParser.java:69)\n" +
          "\tat com.streamsets.pipeline.stage.origin.spooldir.SpoolDirSource.produce(SpoolDirSource.java:566)\n" +
          "\tat com.streamsets.pipeline.stage.origin.spooldir.SpoolDirSource.produce(SpoolDirSource.java:535)\n" +
          "\tat com.streamsets.pipeline.configurablestage.DSource.produce(DSource.java:24)\n" +
          "\tat com.streamsets.pipeline.runner.StageRuntime.execute(StageRuntime.java:149)\n" +
          "\tat com.streamsets.pipeline.runner.StagePipe.process(StagePipe.java:106)\n" +
          "\tat com.streamsets.pipeline.runner.preview.PreviewPipelineRunner.run(PreviewPipelineRunner.java:85)\n" +
          "\tat com.streamsets.pipeline.runner.Pipeline.run(Pipeline.java:98)\n" +
          "\tat com.streamsets.pipeline.runner.preview.PreviewPipeline.run(PreviewPipeline.java:38)",
        record.get("/" + Constants.MESSAGE).getValueAsString());

      record = records.get(2);

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

  @Test(expected = StageException.class)
  public void testTreatStackTraceAsError() throws Exception {
    SpoolDirSource source = createSource(OnParseError.ERROR, 0);
    PushSourceRunner runner = new PushSourceRunner.Builder(SpoolDirDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
      SpoolDirRunnable runnable = source.getSpoolDirRunnable(threadNumber, batchSize, lastSourceOffset);
      runnable.generateBatch(createLogFileWithStackTrace(), "0", 10, batchMaker);
    } finally {
      runner.runDestroy();
    }
  }
}
