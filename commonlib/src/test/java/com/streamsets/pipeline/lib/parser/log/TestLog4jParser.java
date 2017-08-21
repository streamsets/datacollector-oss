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
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

public class TestLog4jParser {

  private static final String LOG_LINE = "2015-03-20 15:53:31,161 DEBUG PipelineConfigurationValidator - " +
    "Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=0";

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

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

  @Test
  public void testParse() throws Exception {
    DataParser parser = getDataParser(LOG_LINE, 1000, 0);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(LOG_LINE, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(141, Long.parseLong(parser.getOffset()));

    Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
    Assert.assertEquals("2015-03-20 15:53:31,161", record.get("/" + Constants.TIMESTAMP).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.SEVERITY));
    Assert.assertEquals("DEBUG", record.get("/" + Constants.SEVERITY).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.CATEGORY));
    Assert.assertEquals("PipelineConfigurationValidator", record.get("/" + Constants.CATEGORY).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.MESSAGE));
    Assert.assertEquals("Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=0",
      record.get("/" + Constants.MESSAGE).getValueAsString());

    parser.close();
  }

  @Test
  public void testParseWithOffset() throws Exception {
    DataParser parser = getDataParser("Hello\n" + LOG_LINE, 1000, 6);
    Assert.assertEquals(6, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::6", record.getHeader().getSourceId());

    Assert.assertEquals(LOG_LINE, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));

    Assert.assertEquals(147, Long.parseLong(parser.getOffset()));

    Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
    Assert.assertEquals("2015-03-20 15:53:31,161", record.get("/" + Constants.TIMESTAMP).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.SEVERITY));
    Assert.assertEquals("DEBUG", record.get("/" + Constants.SEVERITY).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.CATEGORY));
    Assert.assertEquals("PipelineConfigurationValidator", record.get("/" + Constants.CATEGORY).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.MESSAGE));
    Assert.assertEquals("Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=0",
      record.get("/" + Constants.MESSAGE).getValueAsString());

    record = parser.parse();
    Assert.assertNull(record);

    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));
    parser.close();
  }

  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    DataParser parser = getDataParser("Hello\nByte", 1000, 0);
    parser.close();
    parser.parse();
  }

  @Test(expected = DataParserException.class)
  public void testTruncate() throws Exception {
    DataParser parser = getDataParser(LOG_LINE, 25, 0);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }

  @Test(expected = DataParserException.class)
  public void testParseNonLogLine() throws Exception {
    DataParser parser = getDataParser(
      "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] This is a log line that does not confirm to default log4j log format",
      1000, 0);
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    try {
      parser.parse();
    } finally {
      parser.close();
    }
  }

  @Test
  public void testParseLogLineWithStackTrace() throws Exception {

    InputStream is = new ByteArrayInputStream(LOG_LINE_WITH_STACK_TRACE.getBytes());

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(10000)
      .setMode(LogMode.LOG4J)
      .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true)
      .setConfig(LogDataParserFactory.LOG4J_FORMAT_KEY, "%d{ISO8601} %-5p %c{1} - %m")
      .setConfig(LogDataParserFactory.ON_PARSE_ERROR_KEY, OnParseError.INCLUDE_AS_STACK_TRACE)
      .setConfig(LogDataParserFactory.LOG4J_TRIM_STACK_TRACES_TO_LENGTH_KEY, 100)
      .build();

    DataParser parser = factory.getParser("id", is, "0");
    Assert.assertEquals(0, Long.parseLong(parser.getOffset()));
    Record record = parser.parse();
    Assert.assertNotNull(record);

    Assert.assertEquals("id::0", record.getHeader().getSourceId());

    Assert.assertEquals(LOG_LINE_WITH_STACK_TRACE, record.get().getValueAsMap().get("originalLine").getValueAsString());

    Assert.assertFalse(record.has("/truncated"));
    Assert.assertEquals(-1, Long.parseLong(parser.getOffset()));

    Assert.assertTrue(record.has("/" + Constants.TIMESTAMP));
    Assert.assertEquals("2015-03-24 17:49:16,808", record.get("/" + Constants.TIMESTAMP).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.SEVERITY));
    Assert.assertEquals("ERROR", record.get("/" + Constants.SEVERITY).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.CATEGORY));
    Assert.assertEquals("ExceptionToHttpErrorProvider", record.get("/" + Constants.CATEGORY).getValueAsString());

    Assert.assertTrue(record.has("/" + Constants.MESSAGE));
    Assert.assertEquals(ERROR_MSG_WITH_STACK_TRACE, record.get("/" + Constants.MESSAGE).getValueAsString());

    parser.close();
  }

  private DataParser getDataParser(String logLine, int maxObjectLength, int readerOffset) throws DataParserException {
    InputStream is = new ByteArrayInputStream(logLine.getBytes());

    DataParserFactoryBuilder dataParserFactoryBuilder = new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    DataParserFactory factory = dataParserFactoryBuilder
      .setMaxDataLen(maxObjectLength)
      .setMode(LogMode.LOG4J)
      .setOverRunLimit(1000)
      .setConfig(LogDataParserFactory.RETAIN_ORIGINAL_TEXT_KEY, true)
      .setConfig(LogDataParserFactory.LOG4J_FORMAT_KEY, "%d{ISO8601} %-5p %c{1} - %m")
      .build();

    return factory.getParser("id", is, String.valueOf(readerOffset));
  }
}
