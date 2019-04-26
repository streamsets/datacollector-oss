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
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.timeout.ReadTimeoutException;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TCPObjectToRecordHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(TCPObjectToRecordHandler.class);

  /*
   * This runnable is scheduled to handle the timeout to force a batch.
   * The runnable is synchronized with the TCPObjectToRecordHandler instance, same as the addRecord()
   * To handle the case that the addRecord() is executing and runs the batch and the scheduler starts the runnable
   * while the addRecord() is still running, we have the skip() method that will make the runnable to do a No-Op
   * when it obtains the synchronized lock. The addRecord(), on running the batch will call skip() on the runnable.
   */
  private class TimeoutBatchRunnable implements Runnable {
    private final ChannelHandlerContext context;
    private volatile boolean skip;
    private volatile boolean stopScheduling;

    public TimeoutBatchRunnable(ChannelHandlerContext context) {
      this.context = context;
      skip = false;
      stopScheduling = false;
    }

    public void skip() {
      skip = true;
    }

    public boolean stoppedScheduling() {
      return stopScheduling;
    }

    public void stopScheduling() {
      this.stopScheduling = true;
    }

    @Override
    public void run() {
      synchronized (TCPObjectToRecordHandler.this) { // synchronizing with newBatch
        // send batch if not yet sent by addRecord due to max batch size
        if (!skip) {
          newBatch(this.context, false);
        } else {
          LOG.debug(
              "Skipping scheduled timeout newBatch(). It can be due to running it in addRecord as max batch size was " +
                  "reached or channel innactive was called");
        }
        // reschedule timeout
        if (!timeoutBatchRunnable.stoppedScheduling()) {
          // we are scheduling a new timeout run, we need the new runnable
          timeoutBatchRunnable = scheduleTimeoutBatch(this.context, maxWaitTime);
        }
      }
    }

  }

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

  private long totalRecordCount = 0;
  private long lastChannelStart = 0;
  private BatchContext batchContext = null;
  private ScheduledFuture<?> maxWaitTimeFlush;
  private TimeoutBatchRunnable timeoutBatchRunnable;
  private ArrayBlockingQueue<Record> recordsQueue;
  private boolean firstBatch;

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
    this.recordsQueue = new ArrayBlockingQueue<>(3*this.maxBatchSize);
    firstBatch = true;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    // client connection opened
    super.channelActive(ctx);
    firstBatch = true;
    lastChannelStart = getCurrentTime();
    recordsQueue.clear();
    totalRecordCount = 0;
    batchContext = context.startBatch();
    long delay = this.maxWaitTime;
    timeoutBatchRunnable = scheduleTimeoutBatch(ctx, delay);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Client at {} (established) connected to TCP server at {}",
          ctx.channel().remoteAddress().toString(),
          new SimpleDateFormat(LOG_DATE_FORMAT_STR).format(new Date(lastChannelStart))
      );
    }
  }

  private TimeoutBatchRunnable scheduleTimeoutBatch(ChannelHandlerContext ctx, long delay) {
    TimeoutBatchRunnable timeoutBatchRunnable = new TimeoutBatchRunnable(ctx);
    maxWaitTimeFlush = ctx.channel().eventLoop().schedule(
        timeoutBatchRunnable,
        Math.max(delay, 0),
        TimeUnit.MILLISECONDS
    );
    return timeoutBatchRunnable;
  }

  private void cancelMaxWaitTimeTask() {
    if (maxWaitTimeFlush != null) {
      // we need to synchronize this as we want to make sure current timeoutBatchRunnable will run with the correct
      // stopScheduling and skip values
      synchronized (this) {
        timeoutBatchRunnable.stopScheduling();
        timeoutBatchRunnable.skip();
      }
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
    cancelMaxWaitTimeTask();
    //firstBatch needed to send first record to error if there is an exception processing the first record
    while(!recordsQueue.isEmpty() || firstBatch) {
      newBatch(ctx, true);
      firstBatch = false;
    }
    // we null the runnable, just to avoid problems in case channel object is reused by netty for next clients
    timeoutBatchRunnable = null;
    maxWaitTimeFlush = null;
    super.channelInactive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof MessageToRecord) {
      Record record = context.createRecord(generateRecordId());
      ((MessageToRecord) msg).populateRecord(record);
      addRecord(ctx, record);
    } else if (msg instanceof Record) {
      // we already have a Record (ex: from a DataFormatParserDecoder), so just add it
      totalRecordCount++;
      addRecord(ctx, (Record)msg);
    } else {
      ctx.writeAndFlush("Unexpected object received type\n");
      throw new IllegalStateException(String.format(
          "Unexpected object type (%s) found in Netty channel pipeline",
          msg.getClass().getName()
      ));
    }
  }

  private void addRecord(ChannelHandlerContext ctx, Record record) {
    recordsQueue.add(record);
    evaluateElAndSendResponse(
        recordProcessedAckEval,
        recordProcessedAckVars,
        recordProcessedAckExpr,
        ctx,
        ackResponseCharset,
        true,
        "record processed",
        record
    );

    if (recordsQueue.size() >= maxBatchSize) {
      boolean shouldSendBatch = false;
      synchronized (this) {
        // we are holding the synchronized block, so even if the runnable is being executed, it will be locked
        // until this block finishes, as here we are checking if runnable should skip the sned batch
        if (recordsQueue.size() >= maxBatchSize) {
          shouldSendBatch = true;
          timeoutBatchRunnable.skip();
          timeoutBatchRunnable.stopScheduling();
        }
      }

      if (shouldSendBatch) {
        newBatch(ctx, false);
        // cancel current timeout
        maxWaitTimeFlush.cancel(false);
        // reschedule timeout
        timeoutBatchRunnable = scheduleTimeoutBatch(ctx, this.maxWaitTime);
      }
    }
  }

  private void newBatch(ChannelHandlerContext ctx, boolean channelInactive) {
    List<Record> recordsToProcess = new ArrayList<>(maxBatchSize);
    Record lastRecord = null;

    int batchSize = recordsQueue.drainTo(recordsToProcess, maxBatchSize);
    if (!recordsToProcess.isEmpty()) {
      lastRecord = recordsToProcess.get(recordsToProcess.size() -1);
    }
    for (Record record : recordsToProcess) {
      batchContext.getBatchMaker().addRecord(record);
    }

    context.processBatch(batchContext);

    if (!channelInactive) {
      batchCompletedAckVars.addVariable("batchSize", batchSize);
      evaluateElAndSendResponse(
          batchCompletedAckEval,
          batchCompletedAckVars,
          batchCompletedAckExpr,
          ctx,
          ackResponseCharset,
          false,
          "batch completed",
          lastRecord
      );

    }

    batchContext = context.startBatch();
  }

  private void evaluateElAndSendResponse(
      ELEval eval,
      ELVars vars,
      String expression,
      ChannelHandlerContext ctx,
      Charset charset,
      boolean recordLevel,
      String expressionDescription,
      Record lastRecord
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
            batchContext.toError(errorRecord, Errors.TCP_36, expressionDescription, exception);
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

    if (exception instanceof ReadTimeoutException) {
      // ReadTimeoutException has to disconnect from the client
      LOG.debug(
          "ReadTimeoutException caught in TCPObjectToRecordHandler. Closing connection due to read timeout. ",
          exception
      );
      ctx.writeAndFlush("Closing connection due to read timeout\n").addListener(ChannelFutureListener.CLOSE);
    } else {
      ctx.writeAndFlush("Closing channel due to exception was thrown\n");
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

  }

  private String generateRecordId() {
    return String.format("TcpOrigin_%s_%d-%d", context.getPipelineId(), lastChannelStart, totalRecordCount++);
  }

  @VisibleForTesting
  long getCurrentTime() {
    return Clock.systemUTC().millis();
  }
}
