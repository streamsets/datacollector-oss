/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.service.parser;

import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.service.Service;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.lib.parser.log.Constants;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class TestLogParserServiceImpl {

  private static final String DEFAULT_GROK_PATTERN = "%{COMMONAPACHELOG}";
  private final static String COMMON_LOG_FORMAT_LINE = "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326";
  private final static String APACHE_ERROR_LOG_FORMAT_LINE = "[Wed Oct 11 14:32:52 2000] [error] [client 127.0.0.1] client denied " +
      "by server configuration1: /export/home/live/ap/htdocs/test1";
  private final static String APACHE_CUSTOM_LOG_FORMAT_LINE = "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326";
  private static final String CUSTOM_LOG_FORMAT = "%h %l %u [%t] \"%m %U %H\" %>s %b";
  private static final String INVALID_CUSTOM_LOG_FORMAT = "%h %xyz %u %t \"%m %U %H\" %>s %b";
  private final static String COMBINED_LOG_FORMAT_LINE = "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 " +
      "2326 \"http://www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\"";
  private static final String GROK_FORMAT_LINE = "[3223] 26 Feb 23:59:01 Background append only file rewriting started by pid " +
      "19383 [19383] 26 Feb 23:59:01 SYNC append only file rewrite performed ";
  private static final String GROK_PATTERN_DEFINITION =
      "REDISTIMESTAMP %{MONTHDAY} %{MONTH} %{TIME}\n" +
          "REDISLOG \\[%{POSINT:pid}\\] %{REDISTIMESTAMP:timestamp} %{GREEDYDATA:message}";
  private static final String GROK_PATTERN = "%{REDISLOG}";
  private final static String LOG_4J_FORMAT_LINE = "2015-03-20 15:53:31,161 DEBUG PipelineConfigurationValidator - " +
      "Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=0";
  private final static String TTCC_LINE1 =  "176 [main] INFO  org.apache.log4j.examples.Sort - Populating an array of 1 elements in reverse order.";
  private static final String REGEX =
      "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+ \\S+ \\S+)\" (\\d{3}) (\\d+)";
  private static final String INVALID_REGEX =
      "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+ \\S+ \\S+\" (\\d{3}) (\\d+)";
  private static final List<RegExConfig> REGEX_CONFIG = new ArrayList<>();
  private final static String REGEX_FORMAT_LINE = "127.0.0.1 ss h [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326";
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

  static {
    RegExConfig r1 = new RegExConfig();
    r1.fieldPath = "remoteHost";
    r1.group = 1;
    REGEX_CONFIG.add(r1);
    RegExConfig r2 = new RegExConfig();
    r2.fieldPath = "logName";
    r2.group = 2;
    REGEX_CONFIG.add(r2);
    RegExConfig r3 = new RegExConfig();
    r3.fieldPath = "remoteUser";
    r3.group = 3;
    REGEX_CONFIG.add(r3);
    RegExConfig r4 = new RegExConfig();
    r4.fieldPath = "requestTime";
    r4.group = 4;
    REGEX_CONFIG.add(r4);
    RegExConfig r5 = new RegExConfig();
    r5.fieldPath = "request";
    r5.group = 5;
    REGEX_CONFIG.add(r5);
    RegExConfig r6 = new RegExConfig();
    r6.fieldPath = "status";
    r6.group = 6;
    REGEX_CONFIG.add(r6);
    RegExConfig r7 = new RegExConfig();
    r7.fieldPath = "bytesSent";
    r7.group = 7;
    REGEX_CONFIG.add(r7);
  }

  @Mock
  private Service.Context context;

  @InjectMocks
  private LogParserServiceImpl logParserService;

  @Before
  public void setup() {
    Mockito.when(context.createConfigIssue(
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.any(),
        Mockito.anyVararg()
    )).thenAnswer((Answer) invocation -> new ConfigIssue() {
      @Override
      public String toString() {
        StringBuilder stringToReturn = new StringBuilder();
        for (Object argument : invocation.getArguments()) {
          stringToReturn.append(argument.toString());
        }
        return stringToReturn.toString();
      }
    });
    Configuration mockConfig = new Configuration();
    mockConfig.set(
        DataFormatConstants.MAX_RUNNERS_CONFIG_KEY,
        DataFormatConstants.DEFAULT_STRING_BUILDER_POOL_SIZE
    );
    Mockito.when(context.getConfiguration()).thenReturn(mockConfig);
  }

  private LogParserServiceImpl getDataParserService() {
    Mockito.when(context.createRecord(Mockito.anyString())).thenAnswer(new Answer() {

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return RecordCreator.create("dummyStage", (String) invocation.getArguments()[0]);
      }
    });

    logParserService.logParserServiceConfig = new LogParserServiceConfig();

    logParserService.logParserServiceConfig.removeCtrlChars = false;
    logParserService.logParserServiceConfig.customLogFormat = null;
    logParserService.logParserServiceConfig.regex = null;
    logParserService.logParserServiceConfig.fieldPathsToGroupName = null;
    logParserService.logParserServiceConfig.grokPatternDefinition = null;
    logParserService.logParserServiceConfig.grokPatternList = Arrays.asList(DEFAULT_GROK_PATTERN);
    logParserService.logParserServiceConfig.enableLog4jCustomLogFormat = false;
    logParserService.logParserServiceConfig.log4jCustomLogFormat = null;

    return logParserService;
  }

  @Test
  public void testCommonLogFormat() throws StageException, IOException {

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.COMMON_LOG_FORMAT;

    logParserService.init();


    Map<String, Field> map = new LinkedHashMap<>();
    map.put("text", Field.create(COMMON_LOG_FORMAT_LINE));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    Record parsedRecord = logParserServiceImpl.getLogParser(
        record.getHeader().getSourceId(),
        record.get("/text").getValueAsString()
    ).parse();

    Field parsed = parsedRecord.get();
    parsedRecord.set("/log", parsed);

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.CLIENTIP));
    Assert.assertEquals("127.0.0.1", parsedRecord.get("/log/" + Constants.CLIENTIP).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.USER_IDENT));
    Assert.assertEquals("ss", parsedRecord.get("/log/" + Constants.USER_IDENT).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.USER_AUTH));
    Assert.assertEquals("h", parsedRecord.get("/log/" + Constants.USER_AUTH).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.TIMESTAMP));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", parsedRecord.get("/log/" + Constants.TIMESTAMP).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.VERB));
    Assert.assertEquals("GET", parsedRecord.get("/log/" + Constants.VERB).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.REQUEST));
    Assert.assertEquals("/apache_pb.gif", parsedRecord.get("/log/" + Constants.REQUEST).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.HTTPVERSION));
    Assert.assertEquals("1.0", parsedRecord.get("/log/" + Constants.HTTPVERSION).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.RESPONSE));
    Assert.assertEquals("200", parsedRecord.get("/log/" + Constants.RESPONSE).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.BYTES));
    Assert.assertEquals("2326", parsedRecord.get("/log/" + Constants.BYTES).getValueAsString());

  }

  @Test
  public void testErrorLogFormat() throws StageException, IOException {

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.APACHE_ERROR_LOG_FORMAT;

    logParserService.init();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("text", Field.create(APACHE_ERROR_LOG_FORMAT_LINE));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    Record parsedRecord = logParserServiceImpl.getLogParser(
        record.getHeader().getSourceId(),
        record.get("/text").getValueAsString()
    ).parse();

    Field parsed = parsedRecord.get();
    parsedRecord.set("/log", parsed);

    Assert.assertFalse(parsedRecord.has("/log/truncated"));

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.TIMESTAMP));
    Assert.assertEquals("Wed Oct 11 14:32:52 2000", parsedRecord.get("/log/" + Constants.TIMESTAMP).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.LOGLEVEL));
    Assert.assertEquals("error", parsedRecord.get("/log/" + Constants.LOGLEVEL).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.CLIENTIP));
    Assert.assertEquals("127.0.0.1", parsedRecord.get("/log/" + Constants.CLIENTIP).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/" + Constants.MESSAGE));
    Assert.assertEquals("client denied by server configuration1: /export/home/live/ap/htdocs/test1",
        parsedRecord.get("/log/" + Constants.MESSAGE).getValueAsString());

  }

  @Test
  public void testApacheCustomLogFormat() throws StageException, IOException {

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.APACHE_CUSTOM_LOG_FORMAT;
    logParserServiceImpl.logParserServiceConfig.customLogFormat = CUSTOM_LOG_FORMAT;

    logParserService.init();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("text", Field.create(APACHE_CUSTOM_LOG_FORMAT_LINE));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    Record parsedRecord = logParserServiceImpl.getLogParser(
        record.getHeader().getSourceId(),
        record.get("/text").getValueAsString()
    ).parse();

    Field parsed = parsedRecord.get();
    parsedRecord.set("/log", parsed);

    Assert.assertFalse(parsedRecord.has("/log/truncated"));

    Assert.assertTrue(parsedRecord.has("/log/remoteHost"));
    Assert.assertEquals("127.0.0.1", parsedRecord.get("/log/remoteHost").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/logName"));
    Assert.assertEquals("ss", parsedRecord.get("/log/logName").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/remoteUser"));
    Assert.assertEquals("h", parsedRecord.get("/log/remoteUser").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/requestTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", parsedRecord.get("/log/requestTime").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/requestMethod"));
    Assert.assertEquals("GET", parsedRecord.get("/log/requestMethod").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/urlPath"));
    Assert.assertEquals("/apache_pb.gif", parsedRecord.get("/log/urlPath").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/httpversion"));
    Assert.assertEquals("1.0", parsedRecord.get("/log/httpversion").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/status"));
    Assert.assertEquals("200", parsedRecord.get("/log/status").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/log/bytesSent"));
    Assert.assertEquals("2326", parsedRecord.get("/log/bytesSent").getValueAsString());


  }

  @Test
  public void testInvalidApacheCustomLogFormat() {

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.APACHE_CUSTOM_LOG_FORMAT;
    logParserServiceImpl.logParserServiceConfig.customLogFormat = INVALID_CUSTOM_LOG_FORMAT;

    List<ConfigIssue> issues = logParserService.init();

    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("LOG_PARSER_06"));
  }

  @Test
  public void testCombinedLogFormat() throws StageException, IOException {

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.COMBINED_LOG_FORMAT;

    logParserService.init();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("text", Field.create(COMBINED_LOG_FORMAT_LINE));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    Record parsedRecord = logParserServiceImpl.getLogParser(
        record.getHeader().getSourceId(),
        record.get("/text").getValueAsString()
    ).parse();

    Field parsed = parsedRecord.get();
    parsedRecord.set("/", parsed);

    Assert.assertTrue(parsedRecord.has("/" + Constants.CLIENTIP));
    Assert.assertEquals("127.0.0.1", parsedRecord.get("/" + Constants.CLIENTIP).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.USER_IDENT));
    Assert.assertEquals("ss", parsedRecord.get("/" + Constants.USER_IDENT).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.USER_AUTH));
    Assert.assertEquals("h", parsedRecord.get("/" + Constants.USER_AUTH).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.TIMESTAMP));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", parsedRecord.get("/" + Constants.TIMESTAMP).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.VERB));
    Assert.assertEquals("GET", parsedRecord.get("/" + Constants.VERB).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.REQUEST));
    Assert.assertEquals("/apache_pb.gif", parsedRecord.get("/" + Constants.REQUEST).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.HTTPVERSION));
    Assert.assertEquals("1.0", parsedRecord.get("/" + Constants.HTTPVERSION).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.RESPONSE));
    Assert.assertEquals("200", parsedRecord.get("/" + Constants.RESPONSE).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.BYTES));
    Assert.assertEquals("2326", parsedRecord.get("/" + Constants.BYTES).getValueAsString());

  }

  @Test
  public void testGrokFormat() throws StageException, IOException {

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.GROK;
    logParserServiceImpl.logParserServiceConfig.grokPatternDefinition = GROK_PATTERN_DEFINITION;
    logParserServiceImpl.logParserServiceConfig.grokPatternList = Arrays.asList(GROK_PATTERN);

    logParserService.init();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("text", Field.create(GROK_FORMAT_LINE));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    Record parsedRecord = logParserServiceImpl.getLogParser(
        record.getHeader().getSourceId(),
        record.get("/text").getValueAsString()
    ).parse();

    Field parsed = parsedRecord.get();
    parsedRecord.set("/", parsed);

    Assert.assertFalse(parsedRecord.has("/truncated"));

    Assert.assertTrue(parsedRecord.has("/timestamp"));
    Assert.assertEquals("26 Feb 23:59:01", parsedRecord.get("/timestamp").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/pid"));
    Assert.assertEquals("3223", parsedRecord.get("/pid").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/message"));
    Assert.assertEquals("Background append only file rewriting started by pid " +
            "19383 [19383] 26 Feb 23:59:01 SYNC append only file rewrite performed ",
        parsedRecord.get("/message").getValueAsString());

  }

  @Test
  public void testLog4jFormat() throws StageException, IOException {

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.LOG4J;

    logParserService.init();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("text", Field.create(LOG_4J_FORMAT_LINE));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    Record parsedRecord = logParserServiceImpl.getLogParser(
        record.getHeader().getSourceId(),
        record.get("/text").getValueAsString()
    ).parse();

    Field parsed = parsedRecord.get();
    parsedRecord.set("/", parsed);

    Assert.assertFalse(parsedRecord.has("/truncated"));

    Assert.assertTrue(parsedRecord.has("/" + Constants.TIMESTAMP));
    Assert.assertEquals("2015-03-20 15:53:31,161", parsedRecord.get("/" + Constants.TIMESTAMP).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.SEVERITY));
    Assert.assertEquals("DEBUG", parsedRecord.get("/" + Constants.SEVERITY).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.CATEGORY));
    Assert.assertEquals("PipelineConfigurationValidator", parsedRecord.get("/" + Constants.CATEGORY).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.MESSAGE));
    Assert.assertEquals("Pipeline 'test:preview' validation. valid=true, canPreview=true, issuesCount=0",
        parsedRecord.get("/" + Constants.MESSAGE).getValueAsString());

  }

  @Test
  public void testLog4jCustomFormat() throws StageException, IOException {

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.LOG4J;
    logParserServiceImpl.logParserServiceConfig.enableLog4jCustomLogFormat = true;
    logParserServiceImpl.logParserServiceConfig.log4jCustomLogFormat = "%-6r [%15.15t] %-5p %30.30c - %m";

    logParserService.init();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("text", Field.create(TTCC_LINE1));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    Record parsedRecord = logParserServiceImpl.getLogParser(
        record.getHeader().getSourceId(),
        record.get("/text").getValueAsString()
    ).parse();

    Field parsed = parsedRecord.get();
    parsedRecord.set("/", parsed);

    Assert.assertFalse(parsedRecord.has("/truncated"));

    Assert.assertTrue(parsedRecord.has("/" + Constants.RELATIVETIME));
    Assert.assertEquals("176", parsedRecord.get("/" + Constants.RELATIVETIME).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.THREAD));
    Assert.assertEquals("main", parsedRecord.get("/" + Constants.THREAD).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.SEVERITY));
    Assert.assertEquals("INFO", parsedRecord.get("/" + Constants.SEVERITY).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.CATEGORY));
    Assert.assertEquals("org.apache.log4j.examples.Sort",
        parsedRecord.get("/" + Constants.CATEGORY).getValueAsString());

    Assert.assertTrue(parsedRecord.has("/" + Constants.MESSAGE));
    Assert.assertEquals("Populating an array of 1 elements in reverse order.",
        parsedRecord.get("/" + Constants.MESSAGE).getValueAsString());

  }

  @Test
  public void testRegexFormat() throws StageException, IOException {

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.REGEX;
    logParserServiceImpl.logParserServiceConfig.regex = REGEX;
    logParserServiceImpl.logParserServiceConfig.fieldPathsToGroupName = REGEX_CONFIG;

    logParserService.init();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("text", Field.create(REGEX_FORMAT_LINE));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    Record parsedRecord = logParserServiceImpl.getLogParser(
        record.getHeader().getSourceId(),
        record.get("/text").getValueAsString()
    ).parse();

    Field parsed = parsedRecord.get();
    parsedRecord.set("/", parsed);

    Assert.assertFalse(parsedRecord.has("/truncated"));

    Assert.assertTrue(parsedRecord.has("/remoteHost"));
    Assert.assertEquals("127.0.0.1", parsedRecord.get("/remoteHost").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/logName"));
    Assert.assertEquals("ss", parsedRecord.get("/logName").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/remoteUser"));
    Assert.assertEquals("h", parsedRecord.get("/remoteUser").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/requestTime"));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", parsedRecord.get("/requestTime").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/request"));
    Assert.assertEquals("GET /apache_pb.gif HTTP/1.0", parsedRecord.get("/request").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/status"));
    Assert.assertEquals("200", parsedRecord.get("/status").getValueAsString());

    Assert.assertTrue(parsedRecord.has("/bytesSent"));
    Assert.assertEquals("2326", parsedRecord.get("/bytesSent").getValueAsString());

  }

  @Test
  public void testInvalidRegEx() {

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.REGEX;
    logParserServiceImpl.logParserServiceConfig.regex = INVALID_REGEX;
    logParserServiceImpl.logParserServiceConfig.fieldPathsToGroupName = REGEX_CONFIG;

    List<ConfigIssue> issues = logParserService.init();

    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("LOG_PARSER_07"));
  }

  @Test
  public void testInvalidRegGroupNumber() {

    List<RegExConfig> regExConfig = new ArrayList<>();
    regExConfig.addAll(REGEX_CONFIG);
    RegExConfig r8 = new RegExConfig();
    r8.fieldPath = "nonExistingGroup";
    r8.group = 8;
    regExConfig.add(r8);

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.REGEX;
    logParserServiceImpl.logParserServiceConfig.regex = REGEX;
    logParserServiceImpl.logParserServiceConfig.fieldPathsToGroupName = regExConfig;

    List<ConfigIssue> issues = logParserService.init();

    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("LOG_PARSER_08"));
  }

  @Test
  public void testProduceFullFileWithStackTrace() throws Exception {

    LogParserServiceImpl logParserServiceImpl = getDataParserService();
    logParserServiceImpl.logParserServiceConfig.logMode = LogMode.LOG4J;
    logParserServiceImpl.logParserServiceConfig.regex = REGEX;
    logParserServiceImpl.logParserServiceConfig.fieldPathsToGroupName = REGEX_CONFIG;

    List<ConfigIssue> issues = logParserService.init();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("text", Field.create(LOG_LINE_WITH_STACK_TRACE));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      Record parsedRecord = logParserServiceImpl.getLogParser(
          record.getHeader().getSourceId(),
          record.get("/text").getValueAsString()
      ).parse();

      Field parsed = parsedRecord.get();
      parsedRecord.set("/", parsed);
    } catch (DataParserException ex) {
      Assert.assertTrue(ex.getErrorCode().toString().equals("LOG_PARSER_03"));
    }


  }

}
