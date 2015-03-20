/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import org.apache.commons.csv.CSVFormat;

@GenerateResourceBundle
public enum CsvMode implements Label {
  CSV("Default CSV (ignores empty lines)", CSVFormat.DEFAULT),
  RFC4180("RFC4180 CSV", CSVFormat.RFC4180),
  EXCEL("MS Excel CSV", CSVFormat.EXCEL),
  MYSQL("MySQL CSV", CSVFormat.MYSQL),
  TDF("Tab Separated Values", CSVFormat.TDF)
  ;

  private final String label;
  private final CSVFormat format;

  CsvMode(String label, CSVFormat format) {
    this.label = label;
    this.format = format;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public CSVFormat getFormat() {
    return format;
  }

}
