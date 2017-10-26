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
package com.streamsets.pipeline.stage.origin.tcp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.net.MessageToRecord;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
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

  private final ELEval recordProcessedAckEval;
  private final ELVars recordProcessedAckVars;
  private final String recordProcessedAckExpr;
  private final ELEval batchCompletedAckEval;
  private final ELVars batchCompletedAckVars;
  private final String batchCompletedAckExpr;
  private final String timeZoneId;
  private final Charset ackResponseCharset;

  private int batchRecordCount = 0;
  private long totalRecordCount = 0;
  private long lastChannelStart = 0;
  private BatchContext batchContext = null;
  private ScheduledFuture<?> maxWaitTimeFlush;
  private Record lastRecord;

  public TCPObjectToRecordHandler(
      PushSource.Context context,
      int maxBatchSize,
      long maxWaitTime,
      StopPipelineHandler stopPipelineHandler,
      ELEval recordProcessedAckEval,
      ELVars recordProcessedAckVars,
      String recordProcessedAckExpr,
      ELEval batchCompletedAckEval,
      ELVars batchCompletedAckVars,
      String batchCompletedAckExpr,
      String timeZoneId,
      Charset ackResponseCharset
  ) {
    Utils.checkNotNull(context, "context");
    Utils.checkNotNull(stopPipelineHandler, "stopPipelineHandler");
    this.context = context;
    this.maxBatchSize = maxBatchSize;
    this.maxWaitTime = maxWaitTime;
    this.stopPipelineHandler = stopPipelineHandler;
    this.recordProcessedAckEval = recordProcessedAckEval;
    this.recordProcessedAckVars = recordProcessedAckVars;
    this.recordProcessedAckExpr = recordProcessedAckExpr;
    this.batchCompletedAckEval = batchCompletedAckEval;
    this.batchCompletedAckVars = batchCompletedAckVars;
    this.batchCompletedAckExpr = batchCompletedAckExpr;
    this.timeZoneId = timeZoneId;
    this.ackResponseCharset = ackResponseCharset;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    // client connection opened
    super.channelActive(ctx);
    lastChannelStart = getCurrentTime();
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
    maxWaitTimeFlush = ctx.channel().eventLoop().schedule(
        () -> this.newBatch(ctx),
        Math.max(delay, 0),
        TimeUnit.MILLISECONDS
    );
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
      addRecord(ctx, record);
    } else if (msg instanceof Record) {
      // we already have a Record (ex: from a DataFormatParserDecoder), so just add it
      addRecord(ctx, (Record)msg);
    } else {
      throw new IllegalStateException(String.format(
          "Unexpected object type (%s) found in Netty channel pipeline",
          msg.getClass().getName()
      ));
    }
  }

  private void addRecord(ChannelHandlerContext ctx, Record record) {
    batchContext.getBatchMaker().addRecord(record);
    lastRecord = record;
    evaluateElAndSendResponse(
        recordProcessedAckEval,
        recordProcessedAckVars,
        recordProcessedAckExpr,
        ctx,
        ackResponseCharset,
        true,
        "record processed"
    );

    if (++batchRecordCount >= maxBatchSize) {
      newBatch(ctx);
    }
  }

  private void newBatch(ChannelHandlerContext ctx) {
    context.processBatch(batchContext);

    batchCompletedAckVars.addVariable("batchSize", batchRecordCount);
    evaluateElAndSendResponse(
        batchCompletedAckEval,
        batchCompletedAckVars,
        batchCompletedAckExpr,
        ctx,
        ackResponseCharset,
        false,
        "batch completed"
    );

    batchContext = context.startBatch();
    batchRecordCount = 0;
    restartMaxWaitTimeTask(ctx, this.maxWaitTime);
  }

  private void evaluateElAndSendResponse(
      ELEval eval,
      ELVars vars,
      String expression,
      ChannelHandlerContext ctx,
      Charset charset,
      boolean recordLevel,
      String expressionDescription
  ) {
    if (Strings.isNullOrEmpty(expression)) {
      return;
    }
    if (lastRecord != null) {
      RecordEL.setRecordInContext(vars, lastRecord);
    }
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(timeZoneId)));
    TimeEL.setCalendarInContext(vars, calendar);
    TimeNowEL.setTimeNowInContext(vars, Date.from(ZonedDateTime.now().toInstant()));
    final String elResult;
    try {
      elResult = eval.eval(vars, expression, String.class);
      ctx.writeAndFlush(Unpooled.copiedBuffer(elResult, charset));
    } catch (ELEvalException exception) {
      if (LOG.isErrorEnabled()) {
        LOG.error(String.format(
            "ELEvalException caught attempting to evaluate %s expression",
            expressionDescription
        ), exception);
      }

      if (recordLevel) {
        switch (context.getOnErrorRecord()) {
          case DISCARD:
            // do nothing
            break;
          case STOP_PIPELINE:
            if (LOG.isErrorEnabled()) {
              LOG.error(String.format(
                  "ELEvalException caught when evaluating %s expression to send client response; failing pipeline %s" +
                      " as per stage configuration: %s",
                  expressionDescription,
                  context.getPipelineId(),
                  exception.getMessage()
              ), exception);
            }
            stopPipelineHandler.stopPipeline(context.getPipelineId(), exception);
            break;
          case TO_ERROR:
            Record errorRecord = lastRecord != null ? lastRecord : context.createRecord(generateRecordId());
            batchContext.toError(errorRecord, exception);
            break;
        }
      } else {
        context.reportError(Errors.TCP_35, expressionDescription, exception.getMessage(), exception);
      }
    }
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
      OnRecordErrorException errorEx = (OnRecordErrorException) exception;
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
          stopPipelineHandler.stopPipeline(context.getPipelineId(), errorEx);
          break;
        case TO_ERROR:
          batchContext.toError(context.createRecord(generateRecordId()), errorEx);
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
  long getCurrentTime() {
    return Clock.systemUTC().millis();
  }
}
