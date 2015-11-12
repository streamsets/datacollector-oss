/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.mesos;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;

import com.streamsets.pipeline.BootstrapCluster;

public class BootstrapMesosDriver {

  private static final String MESOS_BOOTSTRAP_JAR_REGEX = "streamsets-datacollector-mesos-bootstrap";
  private static final String SDC_MESOS_BASE_DIR = "sdc_mesos";

  /**
   * Bootstrapping the Driver which starts a Spark job on Mesos
   */
  public static void main(String[] args) throws Exception {

    BootstrapCluster.printSystemPropsEnvVariables();
    String mesosHomeDir = System.getenv("MESOS_DIRECTORY");
    String sparkHome = System.getenv("SPARK_HOME");
    // Extract archives from the uber jar
    String[] cmd = {"/bin/bash", "-c",
          "cd " + mesosHomeDir + "; "
        + "mkdir " + SDC_MESOS_BASE_DIR + "; cd " + SDC_MESOS_BASE_DIR + ";"
        + "jar -xf ../" + MESOS_BOOTSTRAP_JAR_REGEX + "*.jar; "
        + "if [ $? -eq 1 ]; then jar -xf " + sparkHome + "/" + MESOS_BOOTSTRAP_JAR_REGEX + "*.jar; fi;"
        + "tar -xf etc.tar.gz; "
        + "mkdir libs; "
        + "tar -xf libs.tar.gz -C libs/; "
        + "tar -xf resources.tar.gz" };
    ProcessBuilder processBuilder = new ProcessBuilder(cmd);
    processBuilder.redirectErrorStream(true);
    Process process = processBuilder.start();
    try (BufferedReader stdOutReader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line = null;
      while ((line = stdOutReader.readLine()) != null) {
        System.out.println(line);
      }
      process.waitFor();
    }
    if (process.exitValue() != 0) {
      throw new IllegalStateException(
        "Process extracting archives from uber jar exited abnormally; check Mesos driver stdout file");
    }
    System.setProperty("SDC_MESOS_BASE_DIR", new File(mesosHomeDir, SDC_MESOS_BASE_DIR).getAbsolutePath());
    final Class<?> clazz = Class.forName("com.streamsets.pipeline.BootstrapClusterStreaming");
    final Method method = clazz.getMethod("main", String[].class);
    method.invoke(null, new Object[]{args});
  }
}
