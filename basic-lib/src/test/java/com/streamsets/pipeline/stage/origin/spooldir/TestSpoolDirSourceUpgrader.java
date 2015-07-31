/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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

    List<Config> upgrade = spoolDirSourceUpgrader.upgrade("x", "y", "z", 1, 2, new ArrayList<Config>());
    Assert.assertEquals(4, upgrade.size());
    Assert.assertEquals("fileCompression", upgrade.get(0).getName());
    Assert.assertEquals("AUTOMATIC", upgrade.get(0).getValue());
    Assert.assertEquals("csvCustomDelimiter", upgrade.get(1).getName());
    Assert.assertEquals('|', upgrade.get(1).getValue());
    Assert.assertEquals("csvCustomEscape", upgrade.get(2).getName());
    Assert.assertEquals('\\', upgrade.get(2).getValue());
    Assert.assertEquals("csvCustomQuote", upgrade.get(3).getName());
    Assert.assertEquals('\"', upgrade.get(3).getValue());

  }

}
