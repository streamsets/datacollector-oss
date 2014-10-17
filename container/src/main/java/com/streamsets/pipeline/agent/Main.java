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
package com.streamsets.pipeline.agent;

import dagger.ObjectGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  public static void main(String[] args) throws Exception {
    try {
      ObjectGraph dagger = ObjectGraph.create(PipelineAgentModule.class);
      final Agent agent = dagger.get(MainAgent.class);

      agent.init();
      final Logger log = LoggerFactory.getLogger(Main.class);
      log.debug("Initialized");
      Thread shutdownHookThread = new Thread("Main.shutdownHook") {
        @Override
        public void run() {
          log.debug("Stopping, reason: SIGTERM (kill)");
          agent.stop();
        }
      };
      Runtime.getRuntime().addShutdownHook(shutdownHookThread);
      log.debug("Starting");
      agent.run();
      Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
      log.debug("Stopping, reason: Shutdown");
      agent.stop();
      System.exit(0);
    } catch (Throwable ex) {
      System.out.println();
      System.out.printf("Abnormal exit: %s", ex.getMessage());
      System.out.printf("Check STDERR for more details");
      System.out.println();
      System.err.println();
      ex.printStackTrace(System.err);
      System.err.println();
      System.exit(1);
    }
  }

}
