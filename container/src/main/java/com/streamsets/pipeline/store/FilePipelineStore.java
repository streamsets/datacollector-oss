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

import com.streamsets.pipeline.agent.RuntimeInfo;
import com.streamsets.pipeline.config.RuntimePipelineConfiguration;
import com.streamsets.pipeline.container.Configuration;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class FilePipelineStore implements PipelineStore {
  private final RuntimeInfo runtimeInfo;
  private final Configuration conf;
  private File storeDir;

  @Inject
  public FilePipelineStore(RuntimeInfo runtimeInfo, Configuration conf) {
    this.runtimeInfo = runtimeInfo;
    this.conf = conf;
  }

  @Override
  public void init() throws IOException {
    storeDir = new File(runtimeInfo.getDataDir(), "pipelines");
    if (!storeDir.exists()) {
      if (!storeDir.mkdirs()) {
        throw new IOException(String.format("Could not create directory '%s'", storeDir.getAbsolutePath()));
      }
    }
  }

  @Override
  public void destroy() {
  }

  @Override
  public void create(String name) throws IOException {

  }

  @Override
  public void delete(String name) throws IOException {

  }

  @Override
  public List<String> getNames() throws IOException {
    return null;
  }

  @Override
  public void save(String name, String user, String tag, String description,
      RuntimePipelineConfiguration pipeline) throws IOException {

  }

  @Override
  public List<RevisionInfo> getRevisions(String name) throws IOException {
    return null;
  }

  @Override
  public RuntimePipelineConfiguration get(String name, String tag) throws IOException {
    return null;
  }
}
