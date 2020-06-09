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
package com.streamsets.pipeline.stage.destination.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.hdfs.common.Errors;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.hdfs.writer.ActiveRecordWriters;
import com.streamsets.pipeline.stage.destination.hdfs.writer.RecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class HdfsTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsTarget.class);

  /**
   * Name of the header that will be searched for directory template (if configured).
   */
  public static final String TARGET_DIRECTORY_HEADER = "targetDirectory";

  private final HdfsTargetConfigBean hdfsTargetConfigBean;
  private ErrorRecordHandler errorRecordHandler;
  private Date batchTime;

  public HdfsTarget(HdfsTargetConfigBean hdfsTargetConfigBean) {
    this.hdfsTargetConfigBean = hdfsTargetConfigBean;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    hdfsTargetConfigBean.init(getContext(), issues);
    return issues;
  }

  private static StageException throwStageException(Exception e) {
    // Hadoop libraries will wrap any non InterruptedException, RuntimeException, Error or IOException to
    // UndeclaredThrowableException so we manually unwrap it here and properly propagate it to user.
    if (e instanceof UndeclaredThrowableException) {
      e = (Exception)e.getCause();
    }

    LOG.error("Exception while talking to HDFS: {}", e.toString(), e);

    if (e instanceof RuntimeException) {
      Throwable cause = e.getCause();
      if (cause != null) {
        return new StageException(Errors.HADOOPFS_13, String.valueOf(cause), cause);
      }
    }
    return new StageException(Errors.HADOOPFS_13, String.valueOf(e), e);
  }


  @Override
  public void destroy() {
    hdfsTargetConfigBean.destroy();
    super.destroy();
  }

  @Override
  public void write(final Batch batch) throws StageException {
    setBatchTime();
    try {
      hdfsTargetConfigBean.getUGI().doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          hdfsTargetConfigBean.getCurrentWriters().purge();
          if (hdfsTargetConfigBean.getLateWriters() != null) {
            hdfsTargetConfigBean.getLateWriters().purge();
          }
          Iterator<Record> it = batch.getRecords();
          if (it.hasNext()) {
            while (it.hasNext()) {
              Record record = it.next();
              try {
                write(record);
              } catch (OnRecordErrorException ex) {
                errorRecordHandler.onError(
                    new OnRecordErrorException(
                        record,
                        ex.getErrorCode(),
                        ex.getParams()
                    )
                );
              }
            }
            hdfsTargetConfigBean.getCurrentWriters().flushAll();
          } else {
            emptyBatch();
          }

          // Issue events that were cached from independent threads running simultaneously to this batch
          hdfsTargetConfigBean.getCurrentWriters().getWriterManager().issueCachedEvents();
          if(hdfsTargetConfigBean.getLateWriters() != null) {
            hdfsTargetConfigBean.getLateWriters().getWriterManager().issueCachedEvents();
          }
          return null;
        }
      });
    } catch (Exception ex) {
      throw throwStageException(ex);
    }
  }

  // we use the emptyBatch() method call to close open files when the late window closes even if there is no more
  // new data.
  protected void emptyBatch() throws StageException {
    setBatchTime();
    try {
      hdfsTargetConfigBean.getUGI().doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          hdfsTargetConfigBean.getCurrentWriters().purge();
          if (hdfsTargetConfigBean.getLateWriters() != null) {
            hdfsTargetConfigBean.getLateWriters().purge();
          }
          return null;
        }
      });
    } catch (Exception ex) {
      throw throwStageException(ex);
    }
  }

  //visible for testing.
  Date setBatchTime() {
    batchTime = new Date();
    return batchTime;
  }

  protected Date getBatchTime() {
    return batchTime;
  }

  protected Date getRecordTime(Record record) throws ELEvalException {
    ELVars variables = getContext().createELVars();
    TimeNowEL.setTimeNowInContext(variables, getBatchTime());
    RecordEL.setRecordInContext(variables, record);
    return hdfsTargetConfigBean.getTimeDriverElEval().eval(variables, hdfsTargetConfigBean.getTimeDriver(), Date.class);
  }

  private void write(Record record) throws StageException {
    try {
      Date recordTime = getRecordTime(record);

      // recordTime may not be null!
      if (recordTime == null) {
        throw new StageException(Errors.HADOOPFS_47, hdfsTargetConfigBean.getTimeDriver());
      }

      if(hdfsTargetConfigBean.dirPathTemplateInHeader
          && !record.getHeader().getAttributeNames().contains(TARGET_DIRECTORY_HEADER)) {
        getContext().toError(record, Errors.HADOOPFS_50);
        return;
      }

      boolean write = true;
      while (write) {
        write = false;
        RecordWriter writer = hdfsTargetConfigBean.getCurrentWriters().get(getBatchTime(), recordTime, record);
        if (writer != null) {
          try {
            writer.write(record);
            //close the file immediately if there are no errors/exceptions
            if (hdfsTargetConfigBean.dataFormat == DataFormat.WHOLE_FILE) {
              hdfsTargetConfigBean.getCurrentWriters().release(writer, false);
            }
            // To avoid double counting, in case of IdleClosedException
            hdfsTargetConfigBean.getToHdfsRecordsCounter().inc();
            hdfsTargetConfigBean.getToHdfsRecordsMeter().mark();
            hdfsTargetConfigBean.getCurrentWriters().release(writer, false);
          } catch (IdleClosedException ex) {
            //For whole file we will not get here.
            hdfsTargetConfigBean.getCurrentWriters().release(writer, false);
            // Try to write again, this time with a new writer
            write = true;
            // No use printing path, since it is a temp path - the real one is created later.
            LOG.debug("Writer was idle closed. Retrying.. ");
          }
        } else {
          switch (hdfsTargetConfigBean.lateRecordsAction) {
            case SEND_TO_ERROR:
              incrementAndMarkLateRecords();
              getContext().toError(record, Errors.HADOOPFS_12, record.getHeader().getSourceId());
              break;
            case SEND_TO_LATE_RECORDS_FILE:
              RecordWriter lateWriter =
                  hdfsTargetConfigBean.getLateWriters().get(getBatchTime(), getBatchTime(), record);
              try {
                lateWriter.write(record);
                if (hdfsTargetConfigBean.dataFormat == DataFormat.WHOLE_FILE) {
                  hdfsTargetConfigBean.getLateWriters().release(lateWriter, false);
                }
                // To avoid double counting, in case of IdleClosedException
                incrementAndMarkLateRecords();
                //We anyway close the late record writers after writing,
                //no need to handle specially for whole file
                hdfsTargetConfigBean.getLateWriters().release(lateWriter, false);
              } catch (IdleClosedException ex) {
                // Try to write again, this time with a new lateWriter
                hdfsTargetConfigBean.getLateWriters().release(lateWriter, false);
                write = true;
                // No use printing path, since it is a temp path - the real one is created later.
                LOG.debug("Writer was idle closed. Retrying.. ");
              }
              break;
            default:
              incrementAndMarkLateRecords();
              throw new RuntimeException(Utils.format("Unknown late records action: {}",
                  hdfsTargetConfigBean.lateRecordsAction));
          }
        }
      }
    } catch (IOException ex) {
      throw new StageException(Errors.HADOOPFS_14, ex.toString(), ex);
    } catch (StageException ex) {
      throw new OnRecordErrorException(ex.getErrorCode(), ex.getParams()); // params includes exception
    }
  }

  private void incrementAndMarkLateRecords() {
    hdfsTargetConfigBean.getLateRecordsCounter().inc();
    hdfsTargetConfigBean.getLateRecordsMeter().mark();
  }

  @VisibleForTesting
  Configuration getHdfsConfiguration() {
    return hdfsTargetConfigBean.getHdfsConfiguration();
  }

  @VisibleForTesting
  CompressionCodec getCompressionCodec() throws StageException {
    return hdfsTargetConfigBean.getCompressionCodec();
  }

  @VisibleForTesting
  long getLateRecordLimitSecs() {
    return hdfsTargetConfigBean.getLateRecordLimitSecs();
  }

  @VisibleForTesting
  protected ActiveRecordWriters getCurrentWriters() {
    return hdfsTargetConfigBean.getCurrentWriters();
  }

  @VisibleForTesting
  protected ActiveRecordWriters getLateWriters() {
    return hdfsTargetConfigBean.getLateWriters();
  }

}
