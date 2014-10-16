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
package com.streamsets.pipeline.container;

import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.api.Module.Info;
import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PreviewPipelineBatch extends PipelineBatch implements PreviewOutput {
  private List<ModuleOutput> output;

  private static class ModuleOutputImpl implements ModuleOutput {
    private Module.Info info;
    private Map<String, List<Record>> output;

    public ModuleOutputImpl(Module.Info info, Map<String, List<Record>> output) {
      this.info = info;
      this.output = output;
    }

    @Override
    public Info getModuleInfo() {
      return info;
    }

    @Override
    public Map<String, List<Record>> getOutput() {
      return output;
    }

    @Override
    public String toString() {
      return String.format("Module '%s' output: %s", info.getInstanceName(), output);
    }
  }

  public PreviewPipelineBatch(String previousBatchId) {
    super(previousBatchId, true);
    output = new ArrayList<ModuleOutput>();
  }

  private Map<String, List<Record>> clone(Map<String, List<Record>> records) {
    Map<String, List<Record>> map = new LinkedHashMap<String, List<Record>>();
    for (Map.Entry<String, List<Record>> entry : records.entrySet()) {
      map.put(entry.getKey(), new ArrayList<Record>(entry.getValue()));
    }
    return map;
  }

  public void pipeCheckPoint(Pipe pipe) {
    if (!(pipe instanceof ObserverPipe)) {
      output.add(new ModuleOutputImpl(pipe.getModuleInfo(), clone(getRecords())));
    }
  }

  @Override
  public List<ModuleOutput> getOutput() {
    return output;
  }
}
