/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestSpoolDirSourceUpgrader {

  @Test
  public void testSpoolDirSourceUpgrader() throws StageException {
    SpoolDirSourceUpgrader spoolDirSourceUpgrader = new SpoolDirSourceUpgrader();

    List<Config> upgrade = spoolDirSourceUpgrader.upgrade("x", "y", "z", 1, 7, new ArrayList<Config>());
    Assert.assertEquals(9, upgrade.size());
    Assert.assertEquals("conf.dataFormatConfig.compression", upgrade.get(0).getName());
    Assert.assertEquals("NONE", upgrade.get(0).getValue());
    Assert.assertEquals("conf.dataFormatConfig.csvCustomDelimiter", upgrade.get(1).getName());
    Assert.assertEquals('|', upgrade.get(1).getValue());
    Assert.assertEquals("conf.dataFormatConfig.csvCustomEscape", upgrade.get(2).getName());
    Assert.assertEquals('\\', upgrade.get(2).getValue());
    Assert.assertEquals("conf.dataFormatConfig.csvCustomQuote", upgrade.get(3).getName());
    Assert.assertEquals('\"', upgrade.get(3).getValue());
    Assert.assertEquals("conf.dataFormatConfig.csvRecordType", upgrade.get(4).getName());
    Assert.assertEquals("LIST", upgrade.get(4).getValue());
    Assert.assertEquals("conf.dataFormatConfig.filePatternInArchive", upgrade.get(5).getName());
    Assert.assertEquals("*", upgrade.get(5).getValue());
    Assert.assertEquals("conf.dataFormatConfig.csvSkipStartLines", upgrade.get(6).getName());
    Assert.assertEquals(0, upgrade.get(6).getValue());
    Assert.assertEquals("conf.allowLateDirectory", upgrade.get(7).getName());
    Assert.assertEquals(false, upgrade.get(7).getValue());
    Assert.assertEquals("conf.useLastModified", upgrade.get(8).getName());
    Assert.assertEquals(FileOrdering.LEXICOGRAPHICAL.name(), upgrade.get(8).getValue());
  }

}
