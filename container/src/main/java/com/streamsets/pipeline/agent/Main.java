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

import com.google.common.annotations.VisibleForTesting;
import dagger.ObjectGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private final Class moduleClass;

  public Main() {
    this(PipelineAgentModule.class);
  }

  @VisibleForTesting
  Main(Class moduleClass) {
    this.moduleClass = moduleClass;
  }

  @VisibleForTesting
  Runtime getRuntime() {
    return Runtime.getRuntime();
  }

  public int doMain() {
    Logger log = null;
    try {
      ObjectGraph dagger = ObjectGraph.create(moduleClass);
      final Agent agent = dagger.get(MainAgent.class);

      dagger.get(LogConfigurator.class).configure();
      log = LoggerFactory.getLogger(Main.class);
      log.info("-----------------------------------------------------------------");
      dagger.get(BuildInfo.class).log(log);
      log.info("-----------------------------------------------------------------");
      dagger.get(RuntimeInfo.class).log(log);
      log.info("-----------------------------------------------------------------");
      log.info("Starting ...");

      agent.init();
      final Logger finalLog = log;
      Thread shutdownHookThread = new Thread("Main.shutdownHook") {
        @Override
        public void run() {
          finalLog.debug("Stopping, reason: SIGTERM (kill)");
          agent.stop();
        }
      };
      getRuntime().addShutdownHook(shutdownHookThread);
      agent.run();
      getRuntime().removeShutdownHook(shutdownHookThread);
      log.debug("Stopping, reason: shutdown");
      agent.stop();
      return 0;
    } catch (Throwable ex) {
      if (log != null) {
        log.error("Abnormal exit: {}", ex.getMessage(), ex);
      }
      System.out.println();
      System.out.printf("Abnormal exit: %s", ex.getMessage());
      System.out.printf("Check STDERR for more details");
      System.out.println();
      System.err.println();
      ex.printStackTrace(System.err);
      System.err.println();
      return 1;
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(new Main().doMain());
  }

}
