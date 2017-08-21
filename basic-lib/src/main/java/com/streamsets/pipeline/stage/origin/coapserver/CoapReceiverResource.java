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
package com.streamsets.pipeline.stage.origin.coapserver;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.Stage;
import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.server.resources.CoapExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class CoapReceiverResource extends CoapResource {
  private static final Logger LOG = LoggerFactory.getLogger(CoapReceiverResource.class);
  private CoapReceiver receiver;
  private final BlockingQueue<Exception> errorQueue;
  private final Meter errorRequestMeter;
  private final Meter requestMeter;
  private final Timer requestTimer;
  private volatile boolean shuttingDown;

  CoapReceiverResource(
      Stage.Context context,
      CoapReceiver receiver,
      BlockingQueue<Exception> errorQueue
  ) {
    super(receiver.getResourceName());
    getAttributes().setTitle(receiver.getResourceName());
    this.receiver = receiver;
    this.errorQueue = errorQueue;
    errorRequestMeter = context.createMeter("errorRequests");
    requestMeter = context.createMeter("requests");
    requestTimer = context.createTimer("requests");
  }

  @Override
  public void handleGET(CoapExchange exchange) {
    handle(exchange);
  }

  @Override
  public void handlePOST(CoapExchange exchange) {
    handle(exchange);
  }

  @Override
  public void handlePUT(CoapExchange exchange) {
    handle(exchange);
  }

  public void handle(CoapExchange exchange) {
    if (shuttingDown) {
      LOG.debug("Shutting down, discarding incoming request from '{}'", exchange.getSourceAddress());
      exchange.respond(CoAP.ResponseCode.SERVICE_UNAVAILABLE);
    } else {
      long start = System.currentTimeMillis();
      LOG.debug("Request accepted from '{}'", exchange.getSourceAddress());
      try {
        if (receiver.process(exchange.getRequestPayload())) {
          exchange.respond(CoAP.ResponseCode.VALID);
          exchange.accept();
          requestMeter.mark();
        } else {
          exchange.respond(CoAP.ResponseCode.INTERNAL_SERVER_ERROR);
          exchange.accept();
          errorRequestMeter.mark();
        }
      } catch (IOException ex) {
        exchange.reject();
        errorQueue.offer(ex);
        errorRequestMeter.mark();
      } finally {
        requestTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
      }
    }
  }

  void setShuttingDown() {
    shuttingDown = true;
  }

}
