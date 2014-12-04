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

public class DateField extends Field<Date> {

  public DateField(Date value) {
    super(Type.DATE, value, true, null);
  }

  public DateField(String value) {
    super(Type.DATE, parse(value), parse(value) != null, value);
  }

  //TODO make customizable
  private static Date parse(String value) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
    try {
      return dateFormat.parse(value);
    } catch (ParseException ex) {
      return null;
    }
  }

}
