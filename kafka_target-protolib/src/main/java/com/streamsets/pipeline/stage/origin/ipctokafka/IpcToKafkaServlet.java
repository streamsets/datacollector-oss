/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.ipctokafka;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.stage.destination.kafka.KafkaConfigBean;
import com.streamsets.pipeline.stage.kafkautils.SdcKafkaProducerPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.iq80.snappy.SnappyFramedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"squid:S2226", "squid:S1989", "squid:S1948"})
public class IpcToKafkaServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(IpcToKafkaServlet.class);

  private final Stage.Context context;
  private final RpcConfigs configs;
  private final KafkaConfigBean kafkaConfigBean;
  private final BlockingQueue<Exception> errorQueue;
  private final int maxRpcRequestSize;
  private final int maxMessageSize;
  private GenericObjectPool<SdcKafkaProducer> kafkaProducerPool;
  private volatile boolean shuttingDown;

  private final Meter invalidRequestMeter;
  private final Meter errorRequestMeter;
  private final Meter requestMeter;
  private final Timer requestTimer;
  private final Timer kafkaTimer;
  private final Meter kafkaMessagesMeter;
  private final Histogram concurrencyHistogram;

  public IpcToKafkaServlet(
      Stage.Context context,
      RpcConfigs configs,
      KafkaConfigBean kafkaConfigBean,
      int kafkaMaxMessageSize,
      BlockingQueue<Exception> errorQueue
  ) {
    this.context = context;
    this.configs = configs;
    this.kafkaConfigBean = kafkaConfigBean;
    this.errorQueue = errorQueue;
    maxRpcRequestSize = configs.maxRpcRequestSize * 1000 * 1000;
    maxMessageSize = kafkaMaxMessageSize * 1000;

    invalidRequestMeter = context.createMeter("invalidRequests");
    errorRequestMeter = context.createMeter("errorRequests");
    requestMeter = context.createMeter("requests");
    requestTimer = context.createTimer("requests");
    kafkaTimer = context.createTimer("kafka");
    kafkaMessagesMeter = context.createMeter("kafkaMessages");

    // context does not have a createHistogram(), TODO open JIRA for that
    concurrencyHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    context
        .getMetrics()
        .register("custom." + context.getPipelineInfo().get(0).getInstanceName() + ".concurrentRequests.histogram",
            concurrencyHistogram);

  }

  @Override
  public void init() throws ServletException {
    super.init();
    int max = configs.maxConcurrentRequests;
    int minIdle = Math.max(1, configs.maxConcurrentRequests / 4);
    int maxIdle = configs.maxConcurrentRequests / 2;
    GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    poolConfig.setMaxTotal(max);
    poolConfig.setMinIdle(minIdle);
    poolConfig.setMaxIdle(maxIdle);
    LOG.debug("Creating Kafka producer pool with max '{}' minIdle '{}' maxIdle '{}'", max, minIdle, maxIdle);
    kafkaProducerPool = new GenericObjectPool<>(new SdcKafkaProducerPooledObjectFactory(kafkaConfigBean), poolConfig);
  }

  @Override
  public void destroy() {
    if (kafkaProducerPool != null) {
      LOG.debug("Destroying Kafka producer pool");
      kafkaProducerPool.close();
    }
    super.destroy();
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String appId = req.getHeader(Constants.X_SDC_APPLICATION_ID_HEADER);
    if (!configs.appId.equals(appId)) {
      LOG.warn("Validation from '{}' invalid appId '{}', rejected", req.getRemoteAddr(), appId);
      resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid 'appId'");
    } else {
      LOG.debug("Validation from '{}', OK", req.getRemoteAddr());
      resp.setHeader(Constants.X_SDC_PING_HEADER, Constants.X_SDC_PING_VALUE);
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }


  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String requestor = req.getRemoteAddr() + ":" + req.getRemotePort();
    if (shuttingDown) {
      LOG.debug("Shutting down, discarding incoming request from '{}'", requestor);
      resp.setStatus(HttpServletResponse.SC_GONE);
    } else {
      String appId = req.getHeader(Constants.X_SDC_APPLICATION_ID_HEADER);
      String compression = req.getHeader(Constants.X_SDC_COMPRESSION_HEADER);
      String contentType = req.getContentType();
      String json1Fragmentable = req.getHeader(Constants.X_SDC_JSON1_FRAGMENTABLE_HEADER);
      if (!Constants.APPLICATION_BINARY.equals(contentType)) {
        invalidRequestMeter.mark();
        resp.sendError(HttpServletResponse.SC_BAD_REQUEST,
            Utils.format("Wrong content-type '{}', expected '{}'", contentType, Constants.APPLICATION_BINARY)
        );
      } else if (!"true".equals(json1Fragmentable)) {
        invalidRequestMeter.mark();
        resp.sendError(HttpServletResponse.SC_BAD_REQUEST,
            Utils.format("RPC client is not using a fragmentable JSON1 encoding, client;s SDC must be upgraded")
        );
      } else if (!configs.appId.equals(appId)) {
        invalidRequestMeter.mark();
        LOG.warn("IPC from '{}' invalid appId '{}', rejected", requestor, appId);
        resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid 'appId'");
      } else {
        long start = System.currentTimeMillis();
        LOG.debug("Request accepted from '{}'", requestor);
        try (InputStream in = req.getInputStream()) {
          InputStream is = in;
          boolean processRequest = true;
          if (compression != null) {
            switch (compression) {
              case Constants.SNAPPY_COMPRESSION:
                is = new SnappyFramedInputStream(is, true);
                break;
              default:
                invalidRequestMeter.mark();
                LOG.warn("Invalid compression '{}' in request from '{}', returning error", compression, requestor);
                resp.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE,
                    "Unsupported compression: " + compression
                );
                processRequest = false;
            }
          }
          if (processRequest) {
            LOG.debug("Processing request from '{}'", requestor);
            List<byte[]> messages = SdcStreamFragmenter.fragment(is, maxMessageSize, maxRpcRequestSize);
            LOG.debug("Request from '{}' broken into '{}' messages", requestor, messages.size());
            long kStart = System.currentTimeMillis();
            SdcKafkaProducer producer = getKafkaProducer();
            long kafkaTime = System.currentTimeMillis() - kStart;
            try {
              for (byte[] message : messages) {
                // we are using round robing partition strategy, partition key is ignored
                kStart = System.currentTimeMillis();
                producer.enqueueMessage(kafkaConfigBean.kafkaConfig.topic, message, "");
                kafkaTime += System.currentTimeMillis() - kStart;
              }
              kStart = System.currentTimeMillis();
              producer.write();
              kafkaTime += System.currentTimeMillis() - kStart;
              resp.setStatus(HttpServletResponse.SC_OK);
              requestMeter.mark();
            } catch (StageException ex ) {
              LOG.warn("Kakfa producer error: {}", ex.toString(), ex);
              errorQueue.offer(ex);
              errorRequestMeter.mark();
              LOG.warn("Error while reading payload from '{}': {}", requestor, ex.toString(), ex);
              resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.toString());
            } finally {
              kStart = System.currentTimeMillis();
              releaseKafkaProducer(producer);
              kafkaTime += System.currentTimeMillis() - kStart;
            }
            kafkaTimer.update(kafkaTime, TimeUnit.MILLISECONDS);
            kafkaMessagesMeter.mark(messages.size());
          }
        } catch (Exception ex) {
          errorRequestMeter.mark();
          LOG.warn("Error while reading payload from '{}': {}", requestor, ex.toString(), ex);
          resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.toString());
        } finally {
          requestTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  SdcKafkaProducer getKafkaProducer() throws Exception {
    try {
      return kafkaProducerPool.borrowObject();
    } finally {
      concurrencyHistogram.update(kafkaProducerPool.getNumActive());
    }
  }

  void releaseKafkaProducer(SdcKafkaProducer producer) {
    kafkaProducerPool.returnObject(producer);
  }

  public void setShuttingDown() {
    shuttingDown = true;
  }

}
