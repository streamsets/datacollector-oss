/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import org.apache.commons.csv.CSVFormat;

public enum CsvFileMode implements BaseEnumChooserValues.EnumWithLabel {
  CSV("Basic CSV", CSVFormat.DEFAULT),
  EXCEL("MS Excel CSV", CSVFormat.EXCEL),
  MYSQL("MySQL CSV", CSVFormat.MYSQL),
  RFC4180("RFC4180 CSV", CSVFormat.RFC4180),
  TDF("Tab Separated Values", CSVFormat.TDF)
  ;

  private final String label;
  private final CSVFormat format;

  CsvFileMode(String label, CSVFormat format) {
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
