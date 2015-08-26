/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.alerts;

public class EmailConstants {

  public static final String CURRENT_VALUE = "currentValue";
  public static final String EXCEPTION_MESSAGE = "exceptionMessage";
  public static final String TIMESTAMP = "timestamp";
  public static final String STREAMSETS_DATA_COLLECTOR_ALERT = "StreamsSets Data Collector Alert - ";
  public static final String ALERT_ERROR_EMAIL_TEMPLATE = "email-template-error-alert.txt";
  public static final String NOTIFY_ERROR_EMAIL_TEMPLATE = "email-template-error-notify.txt";
  public static final String METRIC_EMAIL_TEMPLATE = "email-template-metric.txt";
  public static final String ALERT_VALUE_KEY = "${ALERT_VALUE}";
  public static final String TIME_KEY = "${TIME}";
  public static final String PIPELINE_NAME_KEY = "${PIPELINE_NAME}";
  public static final String DESCRIPTION_KEY = "${DESCRIPTION}";
  public static final String ERROR_CODE = "${ERROR_CODE}";
  public static final String CONDITION_KEY = "${CONDITION}";
  public static final String URL_KEY = "${URL}";
  public static final String DATE_MASK = "yyyy-MM-dd HH:mm:ss";
  public static final String PIPELINE_URL = "/collector/pipeline/";
  public static final String STOPPED_EMAIL_TEMPLATE = "email-template-stopped.txt";
  public static final String MESSAGE_KEY = "${MESSAGE}";

}
