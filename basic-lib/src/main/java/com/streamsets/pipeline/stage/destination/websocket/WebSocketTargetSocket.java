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
package com.streamsets.pipeline.stage.destination.websocket;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


@WebSocket
public class WebSocketTargetSocket {

  private static final Logger LOG = LoggerFactory.getLogger(WebSocketTargetSocket.class);
  private final WebSocketTargetConfig conf;
  private final DataGeneratorFactory generatorFactory;
  private final ErrorRecordHandler errorRecordHandler;
  private final CountDownLatch closeLatch;
  private final Batch batch;

  WebSocketTargetSocket(
      WebSocketTargetConfig conf,
      DataGeneratorFactory generatorFactory,
      ErrorRecordHandler errorRecordHandler,
      Batch batch
  ) {
    this.conf = conf;
    this.generatorFactory = generatorFactory;
    this.errorRecordHandler = errorRecordHandler;
    this.batch = batch;
    this.closeLatch = new CountDownLatch(1);
  }

  boolean awaitClose(long duration, TimeUnit unit) throws InterruptedException {
    return this.closeLatch.await(duration, unit);
  }

  @OnWebSocketClose
  public void onClose(int statusCode, String reason) {
    if (statusCode != StatusCode.NORMAL) {
      LOG.error(Utils.format("WebSocket Connection closed: {} - {}", statusCode, reason));
    }
    this.closeLatch.countDown();
  }

  @OnWebSocketConnect
  public void onConnect(Session session) {
    try {
      List<Future<Void>> responses = new ArrayList<>();
      Iterator<Record> records = batch.getRecords();
      while (records.hasNext()) {
        Record record = records.next();
        ByteArrayOutputStream byteBufferOutputStream = new ByteArrayOutputStream();
        try (DataGenerator dataGenerator = generatorFactory.getGenerator(byteBufferOutputStream)) {
          dataGenerator.write(record);
          dataGenerator.flush();
          if (conf.dataFormat == DataFormat.TEXT) {
            responses.add(session.getRemote().sendStringByFuture(byteBufferOutputStream.toString()));
          } else {
            responses.add(session.getRemote().sendBytesByFuture(ByteBuffer.wrap(byteBufferOutputStream.toByteArray())));
          }
        }
      }

      records = batch.getRecords();
      int recordNum = 0;
      while (records.hasNext()) {
        Record record = records.next();
        try {
          responses.get(recordNum).get(conf.maxRequestCompletionSecs, TimeUnit.SECONDS);
        } catch (Exception e) {
          LOG.error(Errors.HTTP_50.getMessage(), e.toString(), e);
          errorRecordHandler.onError(new OnRecordErrorException(record, Errors.HTTP_50, e.toString()));
        } finally {
          ++recordNum;
        }
      }
    } catch (Throwable t) {
      LOG.error(Errors.HTTP_50.getMessage(), t.toString(), t);
      try {
        errorRecordHandler.onError(Errors.HTTP_50, t, t);
      } catch (StageException e) {
        LOG.error("Error when triggering StageException");
      }
    } finally {
      session.close(StatusCode.NORMAL,"Batch Completed");
    }
  }
}
