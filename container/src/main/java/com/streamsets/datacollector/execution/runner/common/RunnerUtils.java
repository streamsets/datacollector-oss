/**
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.execution.runner.common;

import com.streamsets.datacollector.runner.Pipe;

import java.util.List;

/**
 */
public class RunnerUtils {

  /**
   * Index of the first stage pipe in any pipe list. We know that it must exists as any valid pipeline
   * needs to have at least one stage (one destination) terminating the pipeline.
   */
  private static final int FIRST_STAGE_PIPE = 3;

  /**
   * Return runner id from given runner.
   *
   * @param runner List of pipes representing the runner
   * @return id of the runner
   */
  public static int getRunnerId(List<Pipe> runner) {
    return runner.get(FIRST_STAGE_PIPE).getStage().getContext().getRunnerId();
  }

}
