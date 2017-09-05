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
package com.streamsets.pipeline.stage.origin.websocketserver;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.http.HttpConstants;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class WebSocketReceiverServlet extends WebSocketServlet implements WebSocketCreator {

  private static final Logger LOG = LoggerFactory.getLogger(WebSocketReceiverServlet.class);

  private final WebSocketReceiver receiver;
  private final BlockingQueue<Exception> errorQueue;
  private final Meter invalidRequestMeter;
  private final Meter errorRequestMeter;
  private final Meter requestMeter;
  private final Timer requestTimer;
  private volatile boolean shuttingDown;

  WebSocketReceiverServlet(
      Stage.Context context,
      WebSocketReceiver receiver,
      BlockingQueue<Exception> errorQueue
  ) {
    this.receiver = receiver;
    this.errorQueue = errorQueue;
    invalidRequestMeter = context.createMeter("invalidRequests");
    errorRequestMeter = context.createMeter("errorRequests");
    requestMeter = context.createMeter("requests");
    requestTimer = context.createTimer("requests");
  }

  WebSocketReceiver getReceiver() {
    return receiver;
  }

  @Override
  public void configure(WebSocketServletFactory webSocketServletFactory) {
    webSocketServletFactory.getPolicy().setIdleTimeout(receiver.getIdleTimeout());
    webSocketServletFactory.setCreator(this);
  }

  @Override
  public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse res) {
    String requester = req.getRemoteAddress() + ":" + req.getRemotePort();
    if (isShuttingDown()) {
      LOG.debug("Shutting down, discarding incoming request from '{}'", requester);
      res.setStatusCode(HttpServletResponse.SC_GONE);
      return null;
    }

    try {
      if (validateRequest(req, res)) {
        LOG.debug("Request accepted from '{}'", requester);
        return new WebSocketReceiverSocket(
            requester,
            receiver,
            errorQueue,
            requestMeter,
            invalidRequestMeter,
            errorRequestMeter,
            requestTimer
        );
      }
    } catch (Exception ex) {
      errorQueue.offer(ex);
      errorRequestMeter.mark();
      LOG.warn("Error while processing request payload from '{}': {}", requester, ex.toString(), ex);
      try {
        res.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.toString());
      } catch (IOException e) {
        LOG.warn("Error while sending WebSocket error message: {}", ex.toString(), ex);
      }
    }
    invalidRequestMeter.mark();
    return null;
  }

  private boolean validateRequest(ServletUpgradeRequest req, ServletUpgradeResponse res) throws ServletException, IOException {
    return validateAppId(req, res) && getReceiver().validate(req, res);
  }

  private boolean validateAppId(ServletUpgradeRequest req, ServletUpgradeResponse res) throws ServletException, IOException {
    boolean valid = false;
    String ourAppId = null;
    try {
      ourAppId = getReceiver().getAppId().get();
    } catch (StageException e) {
      throw new IOException("Cant resolve credential value", e);
    }

    String requester = req.getRemoteAddress() + ":" + req.getRemotePort();
    String reqAppId = req.getHeader(HttpConstants.X_SDC_APPLICATION_ID_HEADER);

    if (reqAppId == null && receiver.isAppIdViaQueryParamAllowed()) {
      Map<String,List<String>> parameterMap = req.getParameterMap();
      if (parameterMap != null && parameterMap.containsKey(HttpConstants.SDC_APPLICATION_ID_QUERY_PARAM)) {
        List<String> values = parameterMap.get(HttpConstants.SDC_APPLICATION_ID_QUERY_PARAM);
        if (values != null && values.size() > 0) {
          reqAppId = values.get(0);
        }
      }
    }

    if (reqAppId == null) {
      // Since it is not possible to pass headers in some of the WebSockets client (example: browser WebSocket API)
      // http://stackoverflow.com/questions/4361173/http-headers-in-websockets-client-api
      // check sub-protocol header for APP ID
      List<String> subProtocols = req.getSubProtocols();
      if (subProtocols != null && subProtocols.contains(ourAppId)) {
        reqAppId = ourAppId;
        res.setAcceptedSubProtocol(reqAppId);
      }
    }

    if (reqAppId == null) {
      LOG.warn("Request from '{}' missing appId, rejected", requester);
      res.sendError(HttpServletResponse.SC_FORBIDDEN, "Missing 'appId'");
    } if (reqAppId != null && !getReceiver().getAppId().equals(reqAppId)) {
      LOG.warn("Request from '{}' invalid appId '{}', rejected", requester, reqAppId);
      res.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid 'appId'");
    } else {
      valid = true;
    }
    return valid;
  }

  private boolean isShuttingDown() {
    return shuttingDown;
  }

  void setShuttingDown() {
    shuttingDown = true;
  }
}
