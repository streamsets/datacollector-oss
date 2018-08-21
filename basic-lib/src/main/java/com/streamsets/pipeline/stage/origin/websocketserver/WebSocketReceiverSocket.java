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
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WebSocketReceiverSocket extends WebSocketAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(WebSocketReceiverSocket.class);
  private final String requester;
  private final WebSocketReceiver receiver;
  private final BlockingQueue<Exception> errorQueue;
  private final Meter invalidRequestMeter;
  private final Meter errorRequestMeter;
  private final Meter requestMeter;
  private final Timer requestTimer;
  private Session session;

  public WebSocketReceiverSocket(
      String requester,
      WebSocketReceiver receiver,
      BlockingQueue<Exception> errorQueue,
      Meter requestMeter,
      Meter invalidRequestMeter,
      Meter errorRequestMeter,
      Timer requestTimer
  ) {
    this.requester = requester;
    this.receiver = receiver;
    this.errorQueue = errorQueue;
    this.requestMeter = requestMeter;
    this.invalidRequestMeter = invalidRequestMeter;
    this.errorRequestMeter = errorRequestMeter;
    this.requestTimer = requestTimer;
  }

  WebSocketReceiver getReceiver() {
    return receiver;
  }

  @Override
  public void onWebSocketBinary(byte[] payload, int offset, int len) {
    long start = System.currentTimeMillis();
    try {
      LOG.debug("Processing request from '{}'", requester);
      if (getReceiver().process(this.session, payload, offset, len)) {
        requestMeter.mark();
      } else {
        errorRequestMeter.mark();
      }
    } catch (IOException ex) {
      errorQueue.offer(ex);
      errorRequestMeter.mark();
      LOG.warn("Error while processing request payload from '{}': {}", requester, ex.toString(), ex);
    } finally {
      requestTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void onWebSocketText(String message) {
    long start = System.currentTimeMillis();
    try {
      LOG.debug("Processing request from '{}'", requester);
      getReceiver().process(this.session, message);
      requestMeter.mark();
    } catch (IOException ex) {
      errorQueue.offer(ex);
      errorRequestMeter.mark();
      LOG.warn("Error while processing request payload from '{}': {}", requester, ex.toString(), ex);
    } finally {
      requestTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void onWebSocketClose(int statusCode, String reason) {
    super.onWebSocketClose(statusCode, reason);
  }

  @Override
  public void onWebSocketConnect(Session sess) {
    this.session = sess;
    super.onWebSocketConnect(sess);
  }

  @Override
  public void onWebSocketError(Throwable cause) {
    errorRequestMeter.mark();
    LOG.warn("Error while processing request payload from '{}': {}", requester, cause.getMessage());
  }
}
