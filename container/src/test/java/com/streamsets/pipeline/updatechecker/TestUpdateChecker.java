/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.updatechecker;

import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.prodmanager.PipelineState;
import com.streamsets.pipeline.prodmanager.State;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.util.Configuration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUpdateChecker {

  @Test
  public void testConstructorAndUrl() {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Configuration conf = new Configuration();
    PipelineManager manager = Mockito.mock(PipelineManager.class);
    UpdateChecker checker = new UpdateChecker(runtimeInfo, conf, manager);
    Assert.assertEquals(UpdateChecker.URL_DEFAULT, checker.getUrl().toString());

    conf.set(UpdateChecker.URL_KEY, "http://foo");
    checker = new UpdateChecker(runtimeInfo, conf, manager);
    Assert.assertEquals("http://foo", checker.getUrl().toString());

    conf.set(UpdateChecker.URL_KEY, "");
    checker = new UpdateChecker(runtimeInfo, conf, manager);
    Assert.assertNull(checker.getUrl());

    Assert.assertNull(checker.getUpdateInfo());
  }

  @Test
  public void testUploadInfo() {
    PipelineConfiguration pipelineConf = MockStages.createPipelineConfigurationComplexSourceProcessorTarget();
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getSDCToken()).thenReturn("hello");
    Configuration conf = new Configuration();
    PipelineManager manager = Mockito.mock(PipelineManager.class);
    Mockito.when(manager.getPipelineConfiguration()).thenReturn(pipelineConf);
    UpdateChecker checker = new UpdateChecker(runtimeInfo, conf, manager);
    Map uploadInfo = checker.getUploadInfo();
    Assert.assertNotNull(uploadInfo);
    assertUploadInfo(uploadInfo);
  }

  private void assertUploadInfo(Map uploadInfo) {
    Assert.assertNotNull(uploadInfo.get("sdc.buildInfo"));
    Assert.assertTrue(uploadInfo.get("sdc.buildInfo") instanceof BuildInfo);
    Assert.assertNotNull(uploadInfo.get("sdc.sha256"));
    Assert.assertEquals(UpdateChecker.getSha256("hello"), uploadInfo.get("sdc.sha256"));
    Assert.assertNotNull(uploadInfo.get("sdc.stages"));
    Assert.assertEquals(9, ((List) uploadInfo.get("sdc.stages")).size());
    Assert.assertNotNull(((Map) ((List) uploadInfo.get("sdc.stages")).get(0)).get("name"));
    Assert.assertNotNull(((Map) ((List) uploadInfo.get("sdc.stages")).get(0)).get("version"));
    Assert.assertNotNull(((Map) ((List) uploadInfo.get("sdc.stages")).get(0)).get("library"));
  }

  @Test
  public void testRunPipelineNotRunning() throws Exception {
    PipelineConfiguration pipelineConf = MockStages.createPipelineConfigurationComplexSourceProcessorTarget();
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getSDCToken()).thenReturn("hello");
    Configuration conf = new Configuration();
    PipelineManager manager = Mockito.mock(PipelineManager.class);
    Mockito.when(manager.getPipelineConfiguration()).thenReturn(pipelineConf);
    PipelineState state = Mockito.mock(PipelineState.class);
    Mockito.when(state.getState()).thenReturn(State.STOPPED);
    Mockito.when(manager.getPipelineState()).thenReturn(state);
    UpdateChecker checker = new UpdateChecker(runtimeInfo, conf, manager);
    checker.run();
    Map updateInfo = checker.getUpdateInfo();
    Assert.assertNull(updateInfo);
  }

  @Test
  public void testRunUpdateCheckNotReachable() throws Exception {
    PipelineConfiguration pipelineConf = MockStages.createPipelineConfigurationComplexSourceProcessorTarget();
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getSDCToken()).thenReturn("hello");
    Configuration conf = new Configuration();
    PipelineManager manager = Mockito.mock(PipelineManager.class);
    Mockito.when(manager.getPipelineConfiguration()).thenReturn(pipelineConf);
    PipelineState state = Mockito.mock(PipelineState.class);

    //running unreachable update checker site
    Mockito.when(state.getState()).thenReturn(State.RUNNING);
    Mockito.when(manager.getPipelineState()).thenReturn(state);
    UpdateChecker checker = new UpdateChecker(runtimeInfo, conf, manager);
    checker.run();
    Map updateInfo = checker.getUpdateInfo();
    Assert.assertNull(updateInfo);
  }

    private static class UpdateCheckerServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      Map map = ObjectMapperFactory.get().readValue(req.getInputStream(), Map.class);
      Assert.assertTrue(map.containsKey("sdc.sha256"));
      resp.setContentType(UpdateChecker.APPLICATION_JSON_MIME);
      resp.setStatus(HttpServletResponse.SC_OK);
      map = new HashMap();
      map.put("update", "Hello");
      ObjectMapperFactory.getOneLine().writeValue(resp.getOutputStream(), map);
    }

  }

  @Test
  public void testRunRunningUpdateCheckAvailable() throws Exception {
    PipelineConfiguration pipelineConf = MockStages.createPipelineConfigurationComplexSourceProcessorTarget();

    Server server = new Server(0);
    ServletContextHandler context = new ServletContextHandler();
    context.addServlet(new ServletHolder(new UpdateCheckerServlet()), "/updatecheck");
    context.setContextPath("/");
    server.setHandler(context);
    try {
      server.start();
      int port = server.getURI().getPort();

      RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
      Mockito.when(runtimeInfo.getSDCToken()).thenReturn("hello");
      Configuration conf = new Configuration();
      conf.set(UpdateChecker.URL_KEY, "http://localhost:" + port + "/updatecheck");
      PipelineManager manager = Mockito.mock(PipelineManager.class);
      Mockito.when(manager.getPipelineConfiguration()).thenReturn(pipelineConf);
      PipelineState state = Mockito.mock(PipelineState.class);

      Mockito.when(state.getState()).thenReturn(State.RUNNING);
      Mockito.when(manager.getPipelineState()).thenReturn(state);
      UpdateChecker checker = new UpdateChecker(runtimeInfo, conf, manager);

      checker.run();
      Map updateInfo = checker.getUpdateInfo();
      Assert.assertNotNull(updateInfo);
      Assert.assertEquals("Hello", updateInfo.get("update"));
    } finally {
      server.stop();
    }
  }

}
