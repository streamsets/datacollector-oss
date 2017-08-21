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
package com.streamsets.datacollector.alerts;

import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.ObserverException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.el.RecordEL;

public class AlertsUtil {

  private static final String ALERT_PREFIX = "alert.";
  private static final String USER_PREFIX = "user.";

  private AlertsUtil() {}

  public static boolean evaluateRecord(Record record, String predicate, ELVariables variables,
                                       ELEvaluator elEvaluator) throws ObserverException {
    try {
      RecordEL.setRecordInContext(variables, record);
      return (Boolean) elEvaluator.eval(variables, predicate, Boolean.class);
    } catch (Exception ex) {
      throw new ObserverException(ContainerError.CONTAINER_0400, predicate, record.getHeader().getSourceId(),
        ex.toString(), ex);
    }
  }

  public static boolean evaluateExpression(String predicate, ELVariables variables,
                                       ELEvaluator elEvaluator) throws ObserverException {
    try {
      return (Boolean) elEvaluator.eval(variables, predicate, Boolean.class);
    } catch (Exception ex) {
      throw new ObserverException(ContainerError.CONTAINER_0400, predicate, ex.toString(), ex);
    }
  }

  public static String getAlertGaugeName(String ruleId) {
    return  ALERT_PREFIX + ruleId;
  }

  public static String getUserMetricName(String ruleId) {
    return  USER_PREFIX + ruleId;
  }


}
