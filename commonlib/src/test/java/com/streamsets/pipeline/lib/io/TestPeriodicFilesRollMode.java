/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class TestPeriodicFilesRollMode {

  @Test
  public void testMethods() throws IOException {

    File testDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDir.mkdirs());
    Path f1 = new File(testDir, "xAx").toPath();
    Path f2 = new File(testDir, "xBx").toPath();
    Files.createFile(f1);
    Files.createFile(f2);

    RollMode rollMode = new PeriodicFilesRollModeFactory().get("x.x");
    Assert.assertNull(rollMode.getLiveFileName());
    Assert.assertFalse(rollMode.isFirstAcceptable("x"));
    Assert.assertFalse(rollMode.isFirstAcceptable("xx"));
    Assert.assertTrue(rollMode.isFirstAcceptable(""));
    Assert.assertTrue(rollMode.isFirstAcceptable(null));
    Assert.assertTrue(rollMode.isFirstAcceptable("xCx"));
    Assert.assertFalse(rollMode.isCurrentAcceptable("x"));
    Assert.assertTrue(rollMode.isCurrentAcceptable("xDx"));
    Assert.assertTrue(rollMode.isFileRolled(new LiveFile(f1)));
    Assert.assertFalse(rollMode.isFileRolled(new LiveFile(f2)));

    Assert.assertTrue(rollMode.getComparator().compare(f1, f2) < 0);

  }

}
