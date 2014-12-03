/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.prodmanager;

import com.streamsets.pipeline.api.ErrorId;
import com.streamsets.pipeline.util.PipelineException;

public class PipelineManagerException extends PipelineException {

  public enum ERROR implements ErrorId {
    COULD_NOT_SET_STATE("Could not set state, {}"),
    COULD_NOT_GET_STATE("Could not get state, {}"),
    INVALID_STATE_TRANSITION("Could not change state from {} to {}"),
    COULD_NOT_SET_OFFSET_RUNNING_STATE("Could not set the source offset during a run"),
    COULD_NOT_RESET_OFFSET_RUNNING_STATE("Could not reset the source offset as the pipeline is running"),
    COULD_NOT_CAPTURE_SNAPSHOT_BECAUSE_PIPELINE_NOT_RUNNING("Could not capture snapshot because pipeline is not running"),
    COULD_NOT_GET_ERROR_RECORDS_BECAUSE_PIPELINE_NOT_RUNNING("Could not get error records because pipeline is not running"),
    INVALID_BATCH_SIZE("Invalid batch size supplied {}"),
    COULD_NOT_START_PIPELINE_MANAGER_REASON("Could not start pipeline manager. Reason : {}"),
    PIPELINE_DOES_NOT_EXIST("Pipeline {} does not exist");

    private final String msg;

    ERROR(String msg) {
      this.msg = msg;
    }
    @Override
    public String getMessage() {
      return msg;
    }
  }

  public PipelineManagerException(ERROR id, Object... params) {
    super(id, params);
  }

}
