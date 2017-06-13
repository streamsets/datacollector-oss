/**
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
package com.streamsets.datacollector.main;

import com.streamsets.datacollector.main.Main;
import com.streamsets.datacollector.memory.MemoryUsageCollector;

import java.lang.instrument.Instrumentation;
import java.util.List;

//TODO - Make this implement DataCollector interface
public class DataCollectorMain extends Main {

  public DataCollectorMain() {
    super(MainStandalonePipelineManagerModule.class);
  }

  public DataCollectorMain(Class moduleClass) {
    super(moduleClass);
  }

  public static void setContext(ClassLoader apiCL, ClassLoader containerCL,
                                List<? extends ClassLoader> moduleCLs, Instrumentation instrumentation) {
    MemoryUsageCollector.initialize(instrumentation);
    RuntimeModule.setStageLibraryClassLoaders(moduleCLs);
  }

  public static void main(String[] args) throws Exception {
    System.exit(new DataCollectorMain().doMain());
  }

}
