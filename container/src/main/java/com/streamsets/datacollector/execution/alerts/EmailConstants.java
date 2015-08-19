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
  public static final String ERROR_EMAIL_TEMPLATE = "Error Code \t\t: ERROR_CODE\n" +
    "Time \t\t: TIME_KEY\n" +
    "Pipeline\t: PIPELINE_NAME_KEY\n" +
    "URL\t\t: URL_KEY\n" +
    "Description\t: DESCRIPTION_KEY";
  public static final String METRIC_EMAIL_TEMPLATE = "Value \t\t: ALERT_VALUE_KEY\n" +
    "Time \t\t: TIME_KEY\n" +
    "Pipeline\t: PIPELINE_NAME_KEY\n" +
    "Condition\t: CONDITION_KEY\n" +
    "URL\t\t: URL_KEY";
  public static final String ALERT_VALUE_KEY = "ALERT_VALUE_KEY";
  public static final String TIME_KEY = "TIME_KEY";
  public static final String PIPELINE_NAME_KEY = "PIPELINE_NAME_KEY";
  public static final String DESCRIPTION_KEY = "DESCRIPTION_KEY";
  public static final String ERROR_CODE = "ERROR_CODE";
  public static final String CONDITION_KEY = "CONDITION_KEY";
  public static final String URL_KEY = "URL_KEY";
  public static final String DATE_MASK = "yyyy-MM-dd HH:mm:ss";
  public static final String PIPELINE_URL = "/collector/pipeline/";
  public static final String STOPPED_EMAIL_TEMPLATE = "Message \t\t: MESSAGE\n" +
    "Time \t\t: TIME_KEY\n" +
    "Pipeline\t: PIPELINE_NAME_KEY\n" +
    "URL\t\t: URL_KEY\n";
  public static final String MESSAGE = "MESSAGE";
  public static final String ERROR_MESSAGE = "ERROR_MESSAGE";

}
