/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;

public class NumberField extends Field<BigDecimal> {

  public NumberField(BigDecimal value) {
    super(Type.NUMBER, value, true, null);
  }

  public NumberField(String value) {
    super(Type.LONG, parse(value), parse(value) != null, value);
  }

  //TODO make customizable
  private static BigDecimal parse(String value) {
    DecimalFormatSymbols symbols = new DecimalFormatSymbols();
    symbols.setGroupingSeparator(',');
    symbols.setDecimalSeparator('.');
    String pattern = "#,##0.0#";
    DecimalFormat decimalFormat = new DecimalFormat(pattern, symbols);
    decimalFormat.setParseBigDecimal(true);
    try {
      return (BigDecimal) decimalFormat.parse(value);
    } catch (ParseException ex) {
      return null;
    }
  }

}
