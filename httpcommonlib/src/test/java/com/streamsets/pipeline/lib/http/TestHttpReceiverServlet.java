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
package com.streamsets.pipeline.lib.http;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.tls.CredentialValueBean;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestHttpReceiverServlet {

  @Test
  public void testGetQueryParameters() throws Exception {
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);

    // Just one parameter
    Mockito.when(req.getQueryString()).thenReturn(HttpConstants.X_SDC_APPLICATION_ID_HEADER + "=bob");
    Map<String, String[]> params = HttpReceiverServlet.getQueryParameters(req);
    Assert.assertEquals(params.get(HttpConstants.X_SDC_APPLICATION_ID_HEADER)[0], "bob");

    // Multiple parameters
    Mockito.when(req.getQueryString()).thenReturn("param1=jim&" + HttpConstants.X_SDC_APPLICATION_ID_HEADER + "=bob&param3=bill");
    params = HttpReceiverServlet.getQueryParameters(req);
    Assert.assertEquals(params.get(HttpConstants.X_SDC_APPLICATION_ID_HEADER)[0], "bob");

    // No parameters
    Mockito.when(req.getQueryString()).thenReturn("");
    params = HttpReceiverServlet.getQueryParameters(req);
    Assert.assertEquals(params.get(HttpConstants.X_SDC_APPLICATION_ID_HEADER), null);

    // Missing requested parameter
    Mockito.when(req.getQueryString()).thenReturn("param1=jim&param2=mary");
    params = HttpReceiverServlet.getQueryParameters(req);
    Assert.assertEquals(params.get(HttpConstants.X_SDC_APPLICATION_ID_HEADER), null);
  }

  @Test
  public void testValidateAppId() throws Exception {
    Stage.Context context =
        ContextInfoCreator.createSourceContext("n", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    HttpReceiver receiver = Mockito.mock(HttpReceiverWithFragmenterWriter.class);
    List id = new ArrayList<>(Arrays.asList(new CredentialValueBean("id")));
    Mockito.when(receiver.getAppIds()).thenReturn(id);
    HttpReceiverServlet servlet = new HttpReceiverServlet(context, receiver, null);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    // no ID;
    Assert.assertFalse(servlet.validateAppId(req, res));
    Mockito.verify(res).sendError(Mockito.eq(HttpServletResponse.SC_FORBIDDEN), Mockito.anyString());

    // invalid ID
    Mockito.when(req.getHeader(Mockito.eq(HttpConstants.X_SDC_APPLICATION_ID_HEADER))).thenReturn("invalid");
    Mockito.reset(res);
    Assert.assertFalse(servlet.validateAppId(req, res));
    Mockito.verify(res).sendError(Mockito.eq(HttpServletResponse.SC_FORBIDDEN), Mockito.anyString());

    // valid ID
    Mockito.when(req.getHeader(Mockito.eq(HttpConstants.X_SDC_APPLICATION_ID_HEADER))).thenReturn("id");
    Mockito.reset(res);
    Assert.assertTrue(servlet.validateAppId(req, res));
    Mockito.verifyZeroInteractions(res);
  }

  @Test
  public void testDoGet() throws Exception {
    Stage.Context context =
        ContextInfoCreator.createSourceContext("n", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    HttpReceiver receiver = Mockito.mock(HttpReceiverWithFragmenterWriter.class);
    Mockito.when(receiver.isApplicationIdEnabled()).thenReturn(true);
    HttpReceiverServlet servlet = new HttpReceiverServlet(context, receiver, null);

    servlet = Mockito.spy(servlet);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);



    // invalid ID;
    Mockito.doReturn(false).when(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    servlet.doGet(req, res);
    Mockito.verifyZeroInteractions(res);

    // valid ID
    Mockito.doReturn(true).when(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    Mockito.reset(res);
    servlet.doGet(req, res);
    Mockito.verify(res).setStatus(Mockito.eq(HttpServletResponse.SC_OK));
    Mockito
        .verify(res)
        .setHeader(Mockito.eq(HttpConstants.X_SDC_PING_HEADER), Mockito.eq(HttpConstants.X_SDC_PING_VALUE));
  }

  @Test
  public void testValidatePostRequest() throws Exception {
    Stage.Context context =
        ContextInfoCreator.createSourceContext("n", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    HttpReceiver receiver = Mockito.mock(HttpReceiverWithFragmenterWriter.class);
    List id = new ArrayList<>(Arrays.asList(new CredentialValueBean("id")));
    Mockito.when(receiver.getAppIds()).thenReturn(id);
    Mockito.when(receiver.isApplicationIdEnabled()).thenReturn(true);
    HttpReceiverServlet servlet = new HttpReceiverServlet(context, receiver, null);
    servlet = Mockito.spy(servlet);


    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    // invalid AppId
    Mockito.doReturn(false).when(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    Assert.assertFalse(servlet.validatePostRequest(req, res));
    Mockito.verify(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    Mockito.verifyZeroInteractions(req);
    Mockito.verifyZeroInteractions(res);

    // valid AppID no compression valid receiver
    Mockito.reset(req);
    Mockito.reset(res);
    Mockito.reset(servlet);
    Mockito.doReturn(true).when(receiver).validate(Mockito.eq(req), Mockito.eq(res));
    Mockito.doReturn(true).when(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    Assert.assertTrue(servlet.validatePostRequest(req, res));
    Mockito.verify(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    Mockito.verify(req, Mockito.times(1)).getHeader(Mockito.eq(HttpConstants.X_SDC_COMPRESSION_HEADER));
    Mockito.verifyZeroInteractions(res);
    Mockito.verify(receiver, Mockito.times(1)).validate(Mockito.eq(req), Mockito.eq(res));

    // valid AppID no compression invalid receiver
    Mockito.reset(req);
    Mockito.reset(res);
    Mockito.reset(servlet);
    Mockito.reset(receiver);
    Mockito.doReturn(true).when(receiver).isApplicationIdEnabled();
    Mockito.doReturn(false).when(receiver).validate(Mockito.eq(req), Mockito.eq(res));
    Mockito.doReturn(true).when(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    Assert.assertFalse(servlet.validatePostRequest(req, res));
    Mockito.verify(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    Mockito.verify(req, Mockito.times(1)).getHeader(Mockito.eq(HttpConstants.X_SDC_COMPRESSION_HEADER));
    Mockito.verifyZeroInteractions(res);
    Mockito.verify(receiver, Mockito.times(1)).validate(Mockito.eq(req), Mockito.eq(res));

    // valid AppID with compression valid receiver
    Mockito.reset(req);
    Mockito.reset(res);
    Mockito.reset(servlet);
    Mockito.reset(receiver);
    Mockito.doReturn(true).when(receiver).isApplicationIdEnabled();
    Mockito.doReturn(true).when(receiver).validate(Mockito.eq(req), Mockito.eq(res));
    Mockito.doReturn(true).when(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    Mockito.doReturn(HttpConstants.SNAPPY_COMPRESSION).when(req).getHeader(HttpConstants.X_SDC_COMPRESSION_HEADER);
    Assert.assertTrue(servlet.validatePostRequest(req, res));
    Mockito.verify(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    Mockito.verify(req, Mockito.times(1)).getHeader(Mockito.eq(HttpConstants.X_SDC_COMPRESSION_HEADER));
    Mockito.verify(receiver, Mockito.times(1)).validate(Mockito.eq(req), Mockito.eq(res));

    // valid AppID with compression valid receiver
    Mockito.reset(req);
    Mockito.reset(res);
    Mockito.reset(servlet);
    Mockito.reset(receiver);
    Mockito.doReturn(true).when(receiver).isApplicationIdEnabled();
    Mockito.doReturn(true).when(receiver).validate(Mockito.eq(req), Mockito.eq(res));
    Mockito.doReturn(true).when(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    Mockito.doReturn("invalid-compression").when(req).getHeader(HttpConstants.X_SDC_COMPRESSION_HEADER);
    Assert.assertFalse(servlet.validatePostRequest(req, res));
    Mockito.verify(servlet).validateAppId(Mockito.eq(req), Mockito.eq(res));
    Mockito.verify(req, Mockito.times(1)).getHeader(Mockito.eq(HttpConstants.X_SDC_COMPRESSION_HEADER));
    Mockito.verify(receiver, Mockito.times(0)).validate(Mockito.eq(req), Mockito.eq(res));
    Mockito
        .verify(res, Mockito.times(1))
        .sendError(Mockito.eq(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE), Mockito.anyString());
  }

  @Test
  public void testDoPost() throws Exception {
    Stage.Context context =
        ContextInfoCreator.createSourceContext("n", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    HttpReceiver receiver = Mockito.mock(HttpReceiverWithFragmenterWriter.class);
    List id = new ArrayList<>(Arrays.asList(new CredentialValueBean("id")));
    Mockito.when(receiver.getAppIds()).thenReturn(id);
    Mockito.when(receiver.process(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
    BlockingQueue<Exception> errorQueue = new ArrayBlockingQueue<Exception>(1);
    HttpReceiverServlet servlet = new HttpReceiverServlet(context, receiver, errorQueue);
    servlet = Mockito.spy(servlet);

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);

    // shutting down
    Mockito.doReturn(true).when(servlet).isShuttingDown();
    servlet.doPost(req, res);
    Mockito.verify(res, Mockito.times(1)).setStatus(Mockito.eq(HttpServletResponse.SC_GONE));
    Mockito.verifyNoMoreInteractions(res);

    // invalid post request
    Mockito.reset(res);
    Mockito.doReturn(false).when(servlet).isShuttingDown();
    Mockito.doReturn(false).when(servlet).validatePostRequest(Mockito.eq(req), Mockito.eq(res));
    servlet.doPost(req, res);
    Mockito.verifyNoMoreInteractions(res);

    // valid post request no compression
    ServletInputStream is = Mockito.mock(ServletInputStream.class);
    Mockito.reset(req);
    Mockito.doReturn(is).when(req).getInputStream();
    Mockito.reset(res);
    Mockito.doReturn(false).when(servlet).isShuttingDown();
    Mockito.doReturn(true).when(servlet).validatePostRequest(Mockito.eq(req), Mockito.eq(res));
    servlet.doPost(req, res);
    Mockito.verify(req, Mockito.times(1)).getInputStream();
    Mockito.verify(receiver, Mockito.times(1)).process(Mockito.eq(req), Mockito.eq(is), Mockito.any());
    Mockito.verify(res, Mockito.times(1)).setStatus(Mockito.eq(HttpServletResponse.SC_OK));

    // valid post request with compression
    is = new ServletInputStream() {
      @Override
      public boolean isFinished() {
        return false;
      }

      @Override
      public boolean isReady() {
        return false;
      }

      @Override
      public void setReadListener(ReadListener readListener) {

      }

      @Override
      public int read() throws IOException {
        return -1;
      }
    };
    Mockito.reset(req);
    Mockito.doReturn(is).when(req).getInputStream();
    Mockito
        .doReturn(HttpConstants.SNAPPY_COMPRESSION)
        .when(req)
        .getHeader(Mockito.eq(HttpConstants.X_SDC_COMPRESSION_HEADER));
    Mockito.reset(res);
    Mockito.doReturn(false).when(servlet).isShuttingDown();
    Mockito.doReturn(true).when(servlet).validatePostRequest(Mockito.eq(req), Mockito.eq(res));
    servlet.doPost(req, res);
    Mockito.verify(req, Mockito.times(1)).getInputStream();
    ArgumentCaptor<InputStream> isCaptor = ArgumentCaptor.forClass(InputStream.class);
    Mockito.verify(receiver, Mockito.times(1)).process(Mockito.eq(req), isCaptor.capture(), Mockito.any());

    // we are failing here becuase the stream does not have a valid snappy payload,
    // we use this to test also the exception path
    Mockito
        .verify(res, Mockito.times(1))
        .sendError(Mockito.eq(HttpServletResponse.SC_INTERNAL_SERVER_ERROR), Mockito.anyString());
    Assert.assertEquals(1, errorQueue.size());
  }

}
