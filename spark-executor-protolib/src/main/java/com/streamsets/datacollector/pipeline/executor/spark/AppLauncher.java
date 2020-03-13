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
package com.streamsets.datacollector.pipeline.executor.spark;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEvalException;
import org.apache.spark.launcher.SparkAppHandle;

import java.util.List;

/**
 * Base interface for launching Spark applications on different cluster managers.
 */
public interface AppLauncher {

  /**
   * Verify that the configs are valid for the cluster manager.
   * Initialize {@linkplain com.streamsets.pipeline.api.el.ELVars}, if supported.
   */
  List<Stage.ConfigIssue> init(Stage.Context context, SparkExecutorConfigBean configs);

  /**
   * Launches an application and returns a spark Handle for client code to track application status
   * @return
   */
  SparkAppHandle launchApp(Record record) //NOSONAR
      throws ApplicationLaunchFailureException, ELEvalException;

  /**
   * Wait for the app to complete. In certain cases, like YARN client mode, the submitted app will wait
   * till it is completed before pipeline resumes processing data. Otherwise this needs to be called, to make
   * the pipeline wait. Returns false if the wait times out, else returns true.
   */
  boolean waitForCompletion() throws InterruptedException;

  /**
   * Close and clean up all resources.
   */
  void close();

}
