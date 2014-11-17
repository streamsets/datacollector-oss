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
package com.streamsets.pipeline.main;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.http.WebServerTask;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.state.PipelineManagerTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.CompositeTask;

import javax.inject.Inject;

public class PipelineTask extends CompositeTask {

  @Inject
  public PipelineTask(StageLibraryTask library, PipelineStoreTask store, PipelineManagerTask pipelineManager,
      WebServerTask webServer) {
    super("pipelineNode", ImmutableList.of(library, store, pipelineManager, webServer), true);
  }

}
