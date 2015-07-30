/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
