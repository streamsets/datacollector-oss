/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.bigtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BigtableUtility {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableUtility.class);
  private Process process;
  private static final int PORT = 8279;

  public BigtableUtility() {
  }

  public void setupEnvironment() {

    // first clean up any left over wreckage.
    try {
      Runtime.getRuntime().exec("pkill cbtemulator");
      Thread.sleep(500);
    } catch (Exception ex) {
      LOG.info("setupEnvironment: exception pkill'ing or sleeping {}", ex.toString());
    }

    // need to push this environment variable to the tests can see it...
    //BIGTABLE_EMULATOR_HOST=localhost:8279

    Map<String, String> existing = System.getenv();
    Map<String, String> newenv = new HashMap<>();
    newenv.put("BIGTABLE_EMULATOR_HOST", "localhost:8279");
    newenv.put("GOOGLE_APPLICATION_CREDENTIALS", "/dev/null");
    newenv.putAll(existing);
    try {
      setEnv(newenv);
    } catch (Exception ex) {
      LOG.error("exception setting environment {}", ex.toString());
    }
  }

  // parts of this are from:
  // http://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java
  public static void setEnv(Map<String, String> newenv) throws Exception {
    Map<String, String> env = System.getenv();
    Class[] classes = Collections.class.getDeclaredClasses();
    for (Class cl : classes) {
      if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
        Field field = cl.getDeclaredField("m");
        field.setAccessible(true);
        Object obj = field.get(env);
        Map<String, String> map = (Map<String, String>) obj;
        map.clear();
        map.putAll(newenv);
      }
    }
  }

  public void startEmulator() throws Exception {

    Runtime runTime = Runtime.getRuntime();
    String[] cmd = {
        //TODO: we should pick this up from an env variable.
        "/Users/bob/google-cloud-sdk/platform/bigtable-emulator/cbtemulator", "--host=localhost", "--port=8279"
    };

    process = runTime.exec(cmd);
    Thread.sleep(1000);
    LOG.info("setupEnvironment: after starting emulator (and a short sleep)");

  }

  public void stopEmulator() {
    LOG.info("bobp: stopEmulator: got here");
    if (process != null) {
      process.destroy();
    }
  }

}
