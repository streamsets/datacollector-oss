/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.Label;
import org.apache.commons.csv.CSVFormat;

public enum CsvFileMode implements Label {
  CSV("CSV (ignores empty lines)", CSVFormat.DEFAULT),
  RFC4180("CSV", CSVFormat.RFC4180),
  EXCEL("MS Excel CSV", CSVFormat.EXCEL),
  MYSQL("MySQL CSV", CSVFormat.MYSQL),
  TDF("TSV (tab-separated)", CSVFormat.TDF)
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
