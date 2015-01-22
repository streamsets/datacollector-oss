/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.ObserverException;

public class AlertsUtil {

  public static boolean evaluateRecord(Record record, String predicate, ELEvaluator.Variables variables,
                                       ELEvaluator elEvaluator) throws ObserverException {
    try {
      ELRecordSupport.setRecordInContext(variables, record);
      return (Boolean) elEvaluator.eval(variables, predicate);
    } catch (Exception ex) {
      throw new ObserverException(ContainerError.CONTAINER_0400, predicate, record.getHeader().getSourceId(), ex.getMessage(), ex);
    }
  }

  public static boolean evaluateExpression(String predicate, ELEvaluator.Variables variables,
                                       ELEvaluator elEvaluator) throws ObserverException {
    try {
      return (Boolean) elEvaluator.eval(variables, predicate);
    } catch (Exception ex) {
      throw new ObserverException(ContainerError.CONTAINER_0400, predicate, ex.getMessage(), ex);
    }
  }
}
