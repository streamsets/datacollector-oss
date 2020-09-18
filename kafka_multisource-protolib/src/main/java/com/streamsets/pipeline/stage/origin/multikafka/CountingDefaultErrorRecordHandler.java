/*
 * Copyright 2020 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.multikafka;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;

import java.util.List;

/**
 * Wrapper around DefaultErrorRecordHandler that can count number of passed error records as that is relevant to the
 * logic we have in this origin.
 */
public class CountingDefaultErrorRecordHandler implements ErrorRecordHandler {

  private final ErrorRecordHandler delegate;
  private int errorRecordCount = 0;

  public CountingDefaultErrorRecordHandler(Stage.Context context, ToErrorContext toError) {
    this.delegate = new DefaultErrorRecordHandler(context, toError);
  }

  public int getErrorRecordCount() {
    return errorRecordCount;
  }

  @Override
  public void onError(ErrorCode errorCode, Object... params) {
    delegate.onError(errorCode, params);
  }

  @Override
  public void onError(OnRecordErrorException error) {
    errorRecordCount++;
    delegate.onError(error);
  }

  @Override
  public void onError(List<Record> batch, StageException error) {
    errorRecordCount += batch.size();
    delegate.onError(batch, error);
  }
}
