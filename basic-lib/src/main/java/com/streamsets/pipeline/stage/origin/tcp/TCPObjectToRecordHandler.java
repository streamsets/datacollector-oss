/*
 * Copyright 2017 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.tcp;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.net.MessageToRecord;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TCPObjectToRecordHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(TCPObjectToRecordHandler.class);

  // can't figure out any cleaner way to detect peer reset via Netty
  public static final String RST_PACKET_MESSAGE = "Connection reset by peer";
  public static final String LOG_DATE_FORMAT_STR = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

  private final PushSource.Context context;
  private final int maxBatchSize;
  private final long maxWaitTime;
  private final StopPipelineHandler stopPipelineHandler;

  private int batchRecordCount = 0;
  private long totalRecordCount = 0;
  private long lastChannelStart = 0;
  private long lastBatchStart = 0;
  private BatchContext batchContext = null;
  private ScheduledFuture<?> maxWaitTimeFlush;

  public TCPObjectToRecordHandler(
      PushSource.Context context,
      int maxBatchSize,
      long maxWaitTime,
      StopPipelineHandler stopPipelineHandler
  ) {
    Utils.checkNotNull(context, "context");
    Utils.checkNotNull(stopPipelineHandler, "stopPipelineHandler");
    this.context = context;
    this.maxBatchSize = maxBatchSize;
    this.maxWaitTime = maxWaitTime;
    this.stopPipelineHandler = stopPipelineHandler;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    // client connection opened
    super.channelActive(ctx);
    lastChannelStart = getCurrentTime();
    lastBatchStart = lastChannelStart;
    long delay = this.maxWaitTime;
    restartMaxWaitTimeTask(ctx, delay);
    batchRecordCount = 0;
    totalRecordCount = 0;
    batchContext = context.startBatch();
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Client at {} (established) connected to TCP server at {}",
          ctx.channel().remoteAddress().toString(),
          new SimpleDateFormat(LOG_DATE_FORMAT_STR).format(new Date(lastChannelStart))
      );
    }
  }

  private void restartMaxWaitTimeTask(ChannelHandlerContext ctx, long delay) {
    cancelMaxWaitTimeTask();
    if (delay > 0) {
      maxWaitTimeFlush = ctx.channel().eventLoop().schedule(this::newBatch, delay, TimeUnit.MILLISECONDS);
    } else {
      LOG.warn("Negative maxWaitTimeFlush task scheduled, so ignoring");
    }
  }

  private void cancelMaxWaitTimeTask() {
    if (maxWaitTimeFlush != null && !maxWaitTimeFlush.isCancelled() && !maxWaitTimeFlush.cancel(false)) {
      LOG.warn("Failed to cancel maxWaitTimeFlush task");
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    // client connection closed
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Client at {} (established at {}) disconnected from TCP server after {} records were processed",
          ctx.channel().remoteAddress().toString(),
          new SimpleDateFormat(LOG_DATE_FORMAT_STR).format(new Date(lastChannelStart)),
          totalRecordCount
      );
    }
    super.channelInactive(ctx);
    cancelMaxWaitTimeTask();
    if (batchContext != null) {
      context.processBatch(batchContext);
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof MessageToRecord) {
      Record record = context.createRecord(generateRecordId());
      ((MessageToRecord) msg).populateRecord(record);
      addRecord(record);
    } else if (msg instanceof Record) {
      // we already have a Record (ex: from a DataFormatParserDecoder), so just add it
      addRecord((Record)msg);
    } else {
      throw new IllegalStateException(String.format(
          "Unexpected object type (%s) found in Netty channel pipeline",
          msg.getClass().getName()
      ));
    }

    long waitTimeRemaining = this.maxWaitTime - (getCurrentTime() - lastBatchStart);
    restartMaxWaitTimeTask(ctx, waitTimeRemaining);
  }

  private void addRecord(Record record) {
    batchContext.getBatchMaker().addRecord(record);
    if (++batchRecordCount >= maxBatchSize) {
      newBatch();
    }
  }

  private void newBatch() {
    context.processBatch(batchContext);
    batchContext = context.startBatch();
    lastBatchStart = getCurrentTime();
    batchRecordCount = 0;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
    Throwable exception = t;
    if (exception instanceof DecoderException) {
      // unwrap the Netty decoder exception
      exception = exception.getCause();
    }
    if (exception instanceof IOException && StringUtils.contains(exception.getMessage(), RST_PACKET_MESSAGE)) {
      // client disconnected forcibly
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Client at {} (established at {}) sent RST to server; disconnecting",
            ctx.channel().remoteAddress().toString(),
            new SimpleDateFormat(LOG_DATE_FORMAT_STR).format(new Date(lastChannelStart))
        );
      }
      ctx.close();
      return;
    } else if (exception instanceof DataParserException) {
      exception = new OnRecordErrorException(Errors.TCP_08, exception.getMessage(), exception);
    }

    LOG.error("exceptionCaught in TCPObjectToRecordHandler", exception);
    if (exception instanceof OnRecordErrorException) {
      switch (context.getOnErrorRecord()) {
        case DISCARD:
          break;
        case STOP_PIPELINE:
          if (LOG.isErrorEnabled()) {
            LOG.error(String.format(
                "OnRecordErrorException caught when parsing TCP data; failing pipeline %s as per stage configuration: %s",
                context.getPipelineId(),
                exception.getMessage()
            ), exception);
          }
          stopPipelineHandler.stopPipeline(context.getPipelineId(), (OnRecordErrorException) exception);
          break;
        case TO_ERROR:
          if (exception instanceof Exception) {
            batchContext.toError(context.createRecord(generateRecordId()), (Exception) exception);
          } else {
            // cause is an Error, just need to wrap it
            batchContext.toError(context.createRecord(generateRecordId()), new Exception(
                "Error caught when processing data in TCP source",
                exception
            ));
          }
          break;
      }
    } else {
      context.reportError(Errors.TCP_07, exception.getClass().getName(), exception.getMessage(), exception);
    }
    // Close the connection when an exception is raised.
    ctx.close();
  }

  private String generateRecordId() {
    return String.format("TcpOrigin_%s_%d-%d", context.getPipelineId(), lastChannelStart, totalRecordCount++);
  }

  @VisibleForTesting
  protected long getCurrentTime() {
    return Clock.systemUTC().millis();
  }
}
