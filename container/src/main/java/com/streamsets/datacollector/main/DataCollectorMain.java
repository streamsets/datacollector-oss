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
package com.streamsets.datacollector.main;

import com.streamsets.datacollector.restapi.WebServerAgentCondition;

import java.lang.instrument.Instrumentation;
import java.util.List;
import java.util.concurrent.Callable;

//TODO - Make this implement DataCollector interface
public class DataCollectorMain extends Main {

  public DataCollectorMain() {
    super(MainStandalonePipelineManagerModule.class, null);
  }

  public DataCollectorMain(Object module, Callable<Boolean> taskStopCondition) {
    super(module, taskStopCondition);
  }

  public static void setContext(
      ClassLoader apiCL,
      ClassLoader containerCL,
      List<? extends ClassLoader> moduleCLs,
      Instrumentation instrumentation
  ) {
    RuntimeModule.setStageLibraryClassLoaders(moduleCLs);
  }

  public static void main(String[] args) throws Exception {
    int exitStatus = 0;
    if(!WebServerAgentCondition.canContinue()) {
      exitStatus = new DataCollectorMain(
          MainStandalonePipelineManagerModule.class,
          WebServerAgentCondition::getReceivedCredentials
      ).doMain();
    }
    if (exitStatus == 0) {
      exitStatus = new DataCollectorMain().doMain();
    }
    System.exit(exitStatus);
  }
}
