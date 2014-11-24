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

import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.task.Task;

import java.util.List;

public interface PipelineStoreTask extends Task {

  public PipelineConfiguration create(String name, String description, String user) throws PipelineStoreException;

  public void delete(String name) throws PipelineStoreException;

  public List<PipelineInfo> getPipelines() throws PipelineStoreException;

  public PipelineInfo getInfo(String name) throws PipelineStoreException;

  public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException;

  public PipelineConfiguration save(String name, String user, String tag, String tagDescription,
      PipelineConfiguration pipeline) throws PipelineStoreException;

  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException;

  public boolean hasPipeline(String name);

}
