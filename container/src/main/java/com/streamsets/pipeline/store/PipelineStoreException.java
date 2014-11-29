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
package com.streamsets.pipeline.store;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.util.PipelineException;

public class PipelineStoreException extends PipelineException {

  public static final ID PIPELINE_DOES_NOT_EXIST = new ID("PIPELINE_DOES_NOT_EXIST", "Pipeline '{}' does not exist");
  public static final ID PIPELINE_ALREADY_EXISTS = new ID("PIPELINE_ALREADY_EXISTS", "Pipeline '{}' already exists");
  public static final ID COULD_NOT_CREATE_PIPELINE = new ID("COULD_NOT_CREATE_PIPELINE", "Could not create pipeline '{}', {}");
  public static final ID COULD_NOT_DELETE_PIPELINE = new ID("COULD_NOT_DELETE_PIPELINE", "Could not delete pipeline '{}', {}");
  public static final ID COULD_NOT_SAVE_PIPELINE = new ID("COULD_NOT_SAVE_PIPELINE", "Could not save pipeline '{}', {}");
  public static final ID INVALID_UUID_FOR_PIPELINE = new ID("INVALID_UUID_FOR_PIPELINE", "The provided UUID does not match the stored one, please reload the pipeline '{}'");
  public static final ID COULD_NOT_LOAD_PIPELINE_INFO = new ID("COULD_NOT_LOAD_PIPELINE_INFO", "Could not load pipeline '{}' info, {}");

  public PipelineStoreException(StageException.ID id, Object... params) {
    super(id, params);
  }

}
