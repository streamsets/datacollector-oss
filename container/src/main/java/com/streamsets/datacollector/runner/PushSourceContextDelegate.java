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
package com.streamsets.datacollector.runner;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;

/**
 * Delete for push related methods of PushSource.Context so that they can easily be delegated to a different object.
 *
 * @see PushSource for details on what those methods are expected to do.
 */
public interface PushSourceContextDelegate {

  public BatchContext startBatch();

  public boolean processBatch(BatchContext batchContext, String entityName, String entityOffset);

  public void commitOffset(String entityName, String entityOffset);

}
