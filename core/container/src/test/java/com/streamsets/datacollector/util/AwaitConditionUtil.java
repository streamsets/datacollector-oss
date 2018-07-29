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
package com.streamsets.datacollector.util;

import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.http.WebServerTask;
import org.awaitility.Duration;

import java.util.concurrent.Callable;

import static org.awaitility.Awaitility.given;

public class AwaitConditionUtil {
  private AwaitConditionUtil() {}

  public static Callable<Boolean> numPipelinesEqualTo(final Manager pipelineManager, final int num) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return pipelineManager.getPipelines().size() == num;
      }
    };
  }

  public static Callable<Boolean> desiredPreviewStatus(final Previewer previewer, final PreviewStatus status) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return previewer.getStatus() == status;
      }
    };
  }

  public static Callable<Boolean> desiredPipelineState(final Runner runner, final PipelineStatus status) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return runner.getState().getStatus() == status;
      }
    };
  }

  public static void waitForStart(final WebServerTask ws) {
    given().ignoreExceptions()
        .await()
        .atMost(Duration.TEN_SECONDS)
        .until(isWebServerTaskRunning(ws));
  }

  private static Callable<Boolean> isWebServerTaskRunning(final WebServerTask ws) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        ws.getServerURI();
        return true;
      }
    };
  }
}
