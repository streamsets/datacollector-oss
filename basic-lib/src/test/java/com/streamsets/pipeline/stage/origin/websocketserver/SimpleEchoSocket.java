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

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Basic Echo Client Socket
 */
@WebSocket(maxTextMessageSize = 64 * 1024)
public class SimpleEchoSocket {

  private final CountDownLatch closeLatch;
  public String receivedMessage;

  SimpleEchoSocket() {
    this.closeLatch = new CountDownLatch(1);
  }

  boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
    return this.closeLatch.await(duration,unit);
  }

  @OnWebSocketClose
  public void onClose(int statusCode, String reason) {
    System.out.printf("Connection closed: %d - %s%n",statusCode,reason);
    this.closeLatch.countDown();
  }

  @OnWebSocketConnect
  public void onConnect(Session session) {
    System.out.printf("Got connect: %s%n",session);
    try {
      Future<Void> fut = session.getRemote().sendStringByFuture("{\"field1\" : \"value\"}");
      fut.get(2, TimeUnit.SECONDS); // wait for send to complete.

      session.close(StatusCode.NORMAL,"I'm done");
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  @OnWebSocketMessage
  public void onMessage(String msg) {
    System.out.printf("Got msg: %s%n", msg);
    receivedMessage = msg;
  }
}
