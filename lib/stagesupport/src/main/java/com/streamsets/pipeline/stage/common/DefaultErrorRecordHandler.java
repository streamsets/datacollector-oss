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
package com.streamsets.pipeline.stage.common;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DefaultErrorRecordHandler implements ErrorRecordHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultErrorRecordHandler.class);

  private final ToErrorContext toError;
  private final Stage.Context context;
  private final OnRecordError onRecordError;

  /**
   * Special constructor for internal Data Collector needs.
   *
   * It allows to override the OnRecordError configured in the stage with a custom value.
   *
   * @param onRecordError Overridden on record error action
   * @param context Context of the stage with configuration of what should happen when error record occur.
   * @param toError Error sink into which records will be send if TO_ERROR is configured by user.
   */
  public DefaultErrorRecordHandler(OnRecordError onRecordError, Stage.Context context, ToErrorContext toError) {
    this.context = context;
    this.toError = toError;
    this.onRecordError = onRecordError;
  }

  /**
   * Proper constructor that separate configuration from error sink.
   *
   * @param context Context of the stage with configuration of what should happen when error record occur.
   * @param toError Error sink into which records will be send if TO_ERROR is configured by user.
   */
  public DefaultErrorRecordHandler(Stage.Context context, ToErrorContext toError) {
    this(context.getOnErrorRecord(), context, toError);
  }

  /**
   * Convenience constructor for Source.
   *
   * @param context context that implements both ToErrorContext and Stage.Context
   */
  public DefaultErrorRecordHandler(Source.Context context) {
    this(context, context);
  }

  /**
   * Convenience constructor for Processor.
   *
   * @param context context that implements both ToErrorContext and Stage.Context
   */
  public DefaultErrorRecordHandler(Processor.Context context) {
    this(context, context);
  }

  /**
   * Convenience constructor for Target.
   *
   * @param context context that implements both ToErrorContext and Stage.Context
   */
  public DefaultErrorRecordHandler(Target.Context context) {
    this(context, context);
  }

  @Override
  public void onError(ErrorCode errorCode, Object... params) throws StageException {
    validateGetOnErrorRecord(null, errorCode, params);
    switch (onRecordError) {
      case DISCARD:
        break;
      case TO_ERROR:
        context.reportError(errorCode, params);
        break;
      case STOP_PIPELINE:
        throw new StageException(errorCode, params);
      default:
        throw new IllegalStateException(Utils.format("Unknown OnError value '{}'", onRecordError));
    }
  }

  @Override
  public void onError(OnRecordErrorException error) throws StageException {
    validateGetOnErrorRecord(error, null, null);
    switch (onRecordError) {
      case DISCARD:
        break;
      case TO_ERROR:
        toError.toError(error.getRecord(), error);
        break;
      case STOP_PIPELINE:
        throw error;
      default:
        throw new IllegalStateException(Utils.format("Unknown OnError value '{}'", onRecordError), error);
    }
  }

  @Override
  public void onError(List<Record> batch, StageException error) throws StageException {
    validateGetOnErrorRecord(error, null, null);
    switch (onRecordError) {
      case DISCARD:
        break;
      case TO_ERROR:
        // Add all the records in batch to error since there is no way to figure out which record in batch
        // caused exception.
        for (Record record : batch) {
          toError.toError(record, error);
        }
        break;
      case STOP_PIPELINE:
        throw error;
      default:
        throw new IllegalStateException(Utils.format("Unknown OnError value '{}'", onRecordError), error);
    }
  }

  private void validateGetOnErrorRecord(Exception ex, ErrorCode errorCode, Object ... params) {
    if(onRecordError == null) {
      if(ex != null) {
        LOG.error("Can't propagate exception to error stream", ex);
      }
      if(errorCode != null) {
        LOG.error("Can't propagate error to error stream: {} with params {}", errorCode, params);
      }

      // We throw "nicer" exception when this stage is in place of an error stage, otherwise generally sounding error
      if(context.isErrorStage()) {
        throw new IllegalStateException(Utils.format("Error stage {} itself generated error record, shutting pipeline down to prevent data loss.", context.getStageInfo().getInstanceName()));
      } else {
        throw new IllegalStateException(Utils.format("Component {} doesn't have configured error record action.", context.getStageInfo().getInstanceName()));
      }
    }
  }

}
