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
package com.streamsets.pipeline.mesos;
import com.streamsets.pipeline.BootstrapCluster;

import java.io.File;
import java.lang.reflect.Method;

public class BootstrapMesosDriver {

  private BootstrapMesosDriver() {}

  /**
   * Bootstrapping the Driver which starts a Spark job on Mesos
   */
  public static void main(String[] args) throws Exception {
    BootstrapCluster.printSystemPropsEnvVariables();
    String mesosDir = System.getenv("MESOS_DIRECTORY");
    if (mesosDir == null) {
      throw new IllegalStateException("Expected the env. variable MESOS_DIRECTORY to be defined");
    }
    File mesosHomeDir = new File(mesosDir);
    String sparkDir = System.getenv("SPARK_HOME");
    if (sparkDir == null) {
      throw new IllegalStateException("Expected the env. variable SPARK_HOME to be defined");
    }
    File sparkHomeDir = new File(sparkDir);
    int processExitValue = BootstrapCluster.findAndExtractJar(mesosHomeDir, sparkHomeDir);
    if (processExitValue != 0) {
      throw new IllegalStateException(
        "Process extracting archives from uber jar exited abnormally; check Mesos driver stdout file");
    }
    System.setProperty("SDC_MESOS_BASE_DIR",
      new File(mesosHomeDir, BootstrapCluster.SDC_MESOS_BASE_DIR).getAbsolutePath());
    final Class<?> clazz = Class.forName("com.streamsets.pipeline.BootstrapClusterStreaming");
    final Method method = clazz.getMethod("main", String[].class);
    method.invoke(null, new Object[] { args });
  }
}
