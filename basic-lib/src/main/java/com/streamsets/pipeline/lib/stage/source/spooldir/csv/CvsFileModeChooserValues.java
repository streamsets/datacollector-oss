/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir.csv;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ChooserValues;
import org.apache.commons.csv.CSVFormat;

import java.util.List;
import java.util.Map;

public class CvsFileModeChooserValues implements ChooserValues {

  private static final Map<String, CSVFormat> MAP = ImmutableMap.of(
    "DEFAULT", CSVFormat.DEFAULT,
    "EXCEL", CSVFormat.EXCEL,
    "MYSQL", CSVFormat.MYSQL,
    "RFC4180", CSVFormat.RFC4180,
    "TDF", CSVFormat.TDF
  );

  private static final List<String> VALUES = ImmutableList.copyOf(MAP.keySet());

  @Override
  public List<String> getValues() {
    return VALUES;
  }

  @Override
  public List<String> getLabels() {
    return VALUES;
  }

  public static CSVFormat getCSVFormat(String name) {
    Preconditions.checkNotNull(name, "name");
    return MAP.get(name);
  }
}
