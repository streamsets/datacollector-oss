/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DatetimeField extends Field<Date> {

  public DatetimeField(Date value) {
    super(Type.DATETIME, value, true, null);
  }

  public DatetimeField(String value) {
    super(Type.DATETIME, parse(value), parse(value) != null, value);
  }

  //TODO make customizable
  private static Date parse(String value) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    try {
      return dateFormat.parse(value);
    } catch (ParseException ex) {
      return null;
    }
  }

}
