/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.restservice;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.http.HttpReceiver;
import com.streamsets.pipeline.lib.http.HttpReceiverServlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;

public class RestServiceReceiverServlet extends HttpReceiverServlet {

  RestServiceReceiverServlet(Stage.Context context, HttpReceiver receiver, BlockingQueue<Exception> errorQueue) {
    super(context, receiver, errorQueue);
  }

  public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    if (request.getMethod().equalsIgnoreCase("PATCH")){
      doPatch(request, response);
    } else {
      super.service(request, response);
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    doPost(req, res);
  }

  @Override
  protected void doHead(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    doPost(req, res);
  }

  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    doPost(req, res);
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    doPost(req, res);
  }

  private void doPatch(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    doPost(req, res);
  }

  @Override
  protected void processRequest(HttpServletRequest req, InputStream is, HttpServletResponse resp) throws IOException {
    if (getReceiver().process(req, is, resp)) {
      requestMeter.mark();
    } else {
      resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Record(s) didn't reach all destinations");
      errorRequestMeter.mark();
    }
  }
}
