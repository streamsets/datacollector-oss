/*
 * Copyright 2020 StreamSets Inc.
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

import com.streamsets.datacollector.activation.Activation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActivationUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ActivationUtil.class);
  public static String sparkMaxExecutorsParamName = "spark.maxExecutors";
  protected static int defaultMaxExecutors = 1;

  /**
   * Get the maximum spack executors from activation
   * 1) If activation is not enabled, registered to SCH, return -1 for no limit
   * 2) If activation is enabled and valid, get the max executor numbers
   * 3) otherwise, return 1 for the max executor numbers
   *
   * @param activation Activation Information
   * @return max number of spark executors, -1 for no limits
   */
  public static int getMaxExecutors(Activation activation) {
    int maxExecutors = defaultMaxExecutors;
    try {
      if (activation.isEnabled()) {
        if (activation.getInfo().isValid() && activation.getInfo().getAdditionalInfo().containsKey(sparkMaxExecutorsParamName)) {
          maxExecutors = Integer.parseInt(activation.getInfo().getAdditionalInfo().get(sparkMaxExecutorsParamName).toString());
        }
      } else {
        maxExecutors = -1;
      }
    } catch (NumberFormatException ex) {
      LOG.warn(
          String.format(
              "Invalid Actiation Info at '%s':'%s'",
              sparkMaxExecutorsParamName,
              activation.getInfo().getAdditionalInfo().get(sparkMaxExecutorsParamName).toString()
          ),
          ex.toString()
      );
    }
    return maxExecutors;
  }
}
