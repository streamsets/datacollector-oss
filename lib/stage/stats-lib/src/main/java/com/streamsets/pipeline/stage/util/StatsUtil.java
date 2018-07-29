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
package com.streamsets.pipeline.stage.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsUtil {

  private static final Logger LOG = LoggerFactory.getLogger(StatsUtil.class);

  public static void sleep(int secs) {
    try {
      Thread.sleep(secs * 1000);
    } catch (InterruptedException ex) {
      String msg = "Interrupted while attempting to fetch latest Metrics from DPM";
      LOG.error(msg);
      throw new RuntimeException(msg, ex);
    }
  }
}
