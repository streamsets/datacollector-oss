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

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;

/**
 * Supports stopping an SDC pipeline by its ID, specifically in response to its configured behavior for error record
 * handling (i.e. an {@link OnRecordErrorException}
 */
public interface StopPipelineHandler {

  /**
   * Stop the pipeline (by whatever mechanism is defined)
   *
   * @param pipelineId the SDC pipeline ID to stop
   * @param error the Exception that triggered this stop, should not be null
   */
  void stopPipeline(String pipelineId, StageException error);
}
