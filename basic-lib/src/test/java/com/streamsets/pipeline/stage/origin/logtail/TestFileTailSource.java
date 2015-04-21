/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.log.Constants;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class TestFileTailSource {

  @Test(expected = StageException.class)
  public void testInitDirDoesNotExist() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());

    FileInfo fileInfo = new FileInfo();
    fileInfo.dirName = testDataDir.getAbsolutePath();
    fileInfo.file = "logFile.txt";
    fileInfo.fileRollMode = FilesRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.periodicFileRegEx = "";
    FileTailSource source = new FileTailSource(DataFormat.TEXT, "UTF-8", Arrays.asList(fileInfo), 1024, 25, 1, null,
                                               false, null, null, null, null, null, false, null);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane")
        .build();
    runner.runInit();
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
    fileInfo.dirName = testDataDir.getAbsolutePath();
    fileInfo.file = "logFile.txt";
    fileInfo.fileRollMode = FilesRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.periodicFileRegEx = "";
    FileTailSource source = new FileTailSource(DataFormat.TEXT, "UTF-8", Arrays.asList(fileInfo), 1024, 25, 1, null,
                                               false, null, null, null, null, null, false, null);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane")
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
      record = output.getRecords().get("lane").get(1);
      Assert.assertEquals("LAST", record.get("/text").getValueAsString());
      record = output.getRecords().get("lane").get(2);
      Assert.assertEquals("HELLO", record.get("/text").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  private static final Charset UTF8 = Charset.forName("UTF-8");

  @Test
  public void testTailLogMultipleDirs() throws Exception {
    File testDataDir1 = new File("target", UUID.randomUUID().toString());
    File testDataDir2 = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir1.mkdirs());
    Assert.assertTrue(testDataDir2.mkdirs());
    Files.write(new File(testDataDir1, "log1.txt").toPath(), Arrays.asList("Hello"), UTF8);
    Files.write(new File(testDataDir2, "log2.txt").toPath(), Arrays.asList("Hola"), UTF8);

    FileInfo fileInfo1 = new FileInfo();
    fileInfo1.dirName = testDataDir1.getAbsolutePath();
    fileInfo1.file = "log1.txt";
    fileInfo1.fileRollMode = FilesRollMode.REVERSE_COUNTER;
    fileInfo1.firstFile = "";
    fileInfo1.periodicFileRegEx = "";
    FileInfo fileInfo2 = new FileInfo();
    fileInfo2.dirName = testDataDir2.getAbsolutePath();
    fileInfo2.file = "log2.txt";
    fileInfo2.fileRollMode = FilesRollMode.REVERSE_COUNTER;
    fileInfo2.firstFile = "";
    fileInfo2.periodicFileRegEx = "";
    FileTailSource source = new FileTailSource(DataFormat.TEXT, "UTF-8", Arrays.asList(fileInfo1, fileInfo2), 1024, 25,
                                               1, null, false, null, null, null, null, null, false, null);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Record record = output.getRecords().get("lane").get(0);
      Assert.assertEquals("Hello", record.get("/text").getValueAsString());
      record = output.getRecords().get("lane").get(1);
      Assert.assertEquals("Hola", record.get("/text").getValueAsString());
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
    fileInfo.dirName = testDataDir.getAbsolutePath();
    fileInfo.file = "logFile.txt";
    fileInfo.fileRollMode = FilesRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.periodicFileRegEx = "";
    FileTailSource source = new FileTailSource(DataFormat.TEXT, "UTF-8", Arrays.asList(fileInfo), 7, 1, 1, null,
                                               false, null, null, null, null, null, false, null);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane")
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
  public void testTailJson() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    File logFile = new File(testDataDir, "logFile.txt");

    FileInfo fileInfo = new FileInfo();
    fileInfo.dirName = testDataDir.getAbsolutePath();
    fileInfo.file = "logFile.txt";
    fileInfo.fileRollMode = FilesRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.periodicFileRegEx = "";
    FileTailSource source = new FileTailSource(DataFormat.JSON, "UTF-8", Arrays.asList(fileInfo), 1024, 25, 1, null,
                                               false, null, null, null, null, null, false, null);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    Files.write(logFile.toPath(), Arrays.asList("{\"a\": 1}", "[{\"b\": 2}]"), Charset.forName("UTF-8"));
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
    fileInfo.dirName = testDataDir.getAbsolutePath();
    fileInfo.file = "logFile.txt";
    fileInfo.fileRollMode = FilesRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.periodicFileRegEx = "";
    FileTailSource source = new FileTailSource(DataFormat.LOG, "UTF-8", Arrays.asList(fileInfo), 1024, 25, 1,
                                               LogMode.LOG4J, true, null, null, null, null, null, false, null);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
      .addOutputLane("lane")
      .build();
    runner.runInit();
    Files.write(logFile.toPath(), Arrays.asList(LINE1, LINE2), Charset.forName("UTF-8"));
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

  @Test(expected = DataParserException.class)
  public void testTailLogFormatStackTrace() throws Exception {
    File testDataDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDataDir.mkdirs());
    File logFile = new File(testDataDir, "logFile.txt");

    FileInfo fileInfo = new FileInfo();
    fileInfo.dirName = testDataDir.getAbsolutePath();
    fileInfo.file = "logFile.txt";
    fileInfo.fileRollMode = FilesRollMode.REVERSE_COUNTER;
    fileInfo.firstFile = "";
    fileInfo.periodicFileRegEx = "";
    FileTailSource source = new FileTailSource(DataFormat.LOG, "UTF-8", Arrays.asList(fileInfo), 2048, 100, 1,
                                               LogMode.LOG4J, true, null, null, null, null, null, false, null);
    SourceRunner runner = new SourceRunner.Builder(FileTailDSource.class, source)
      .addOutputLane("lane")
      .build();
    runner.runInit();
    Files.write(logFile.toPath(), Arrays.asList(LOG_LINE_WITH_STACK_TRACE), Charset.forName("UTF-8"));
    try {
      runner.runProduce(null, 100);
    } finally {
      runner.runDestroy();
    }
  }

}
