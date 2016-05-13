/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.common;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class DefaultErrorRecordHandler implements ErrorRecordHandler {
  private final Stage.Context context;

  public DefaultErrorRecordHandler(Stage.Context context) {
    this.context = context;
  }

  @Override
  public void onError(ErrorCode errorCode, Object... params) throws StageException {
    switch (context.getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        context.reportError(errorCode, params);
        break;
      case STOP_PIPELINE:
        throw new StageException(errorCode, params);
      default:
        throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
            context.getOnErrorRecord()));
    }
  }

  @Override
  public void onError(OnRecordErrorException error) throws StageException {
    switch (context.getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        context.toError(error.getRecord(), error);
        break;
      case STOP_PIPELINE:
        throw error;
      default:
        throw new IllegalStateException(
            Utils.format("Unknown OnError value '{}'", context.getOnErrorRecord(), error)
        );
    }
  }

  @Override
  public void onError(List<Record> batch, StageException error) throws StageException {
    switch (context.getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        // Add all the records in batch to error since there is no way to figure out which record in batch
        // caused exception.
        for (Record record : batch) {
          context.toError(record, error);
        }
        break;
      case STOP_PIPELINE:
        throw error;
      default:
        throw new IllegalStateException(
            Utils.format("Unknown OnError value '{}'", context.getOnErrorRecord(), error)
        );
    }
  }
}
