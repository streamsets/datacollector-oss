/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Map;
import java.util.UUID;

public class TestLogRollMode {

  @Test
  public void testMethods() throws IOException {

    File testDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDir.mkdirs());
    Path f1 = new File(testDir, "x.1").toPath();
    Path f2 = new File(testDir, "x.2").toPath();
    Files.createFile(f1);
    Files.createFile(f2);

    RollMode rollMode = LogRollMode.ALPHABETICAL;
    Assert.assertEquals("foo", rollMode.getLiveFileName("foo"));
    Assert.assertFalse(rollMode.isFirstAcceptable("x", "x"));
    Assert.assertFalse(rollMode.isFirstAcceptable("x", "xx"));
    Assert.assertTrue(rollMode.isFirstAcceptable("x", ""));
    Assert.assertTrue(rollMode.isFirstAcceptable("x", null));
    Assert.assertTrue(rollMode.isFirstAcceptable("x", "x.1"));
    Assert.assertFalse(rollMode.isCurrentAcceptable("x", "x"));
    Assert.assertTrue(rollMode.isCurrentAcceptable("x", "y"));
    Assert.assertTrue(rollMode.isCurrentAcceptable("x", null));
    Assert.assertFalse(rollMode.isFileRolled(new LiveFile(f1), new LiveFile(f1)));
    Assert.assertTrue(rollMode.isFileRolled(new LiveFile(f2), new LiveFile(f1)));

    //for ALPHABETICAL and all DATE_...
    Assert.assertTrue(rollMode.getComparator("x").compare(f1, f2) < 0);

    //for REVERSE_COUNTER
    Assert.assertTrue(rollMode.getComparator("x").compare(f1, f2) < 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRolledFilesModePatterns() throws Exception {
    String name = new File("target/" + UUID.randomUUID().toString(), "my.log").getAbsolutePath();

    Map<LogRollMode, String> MATCH = (Map) ImmutableMap.builder()
                                                       .put(LogRollMode.ALPHABETICAL, name + ".a")
                                                       .put(LogRollMode.REVERSE_COUNTER, name + ".124")
                                                       .put(LogRollMode.DATE_YYYY_MM, name + ".2015-12")
                                                       .put(LogRollMode.DATE_YYYY_MM_DD, name + ".2015-12-01")
                                                       .put(LogRollMode.DATE_YYYY_MM_DD_HH, name + ".2015-12-01-23")
                                                       .put(LogRollMode.DATE_YYYY_MM_DD_HH_MM, name + ".2015-12-01-23-59")
                                                       .put(LogRollMode.DATE_YYYY_WW, name + ".2015-40")
                                                       .build();

    Map<LogRollMode, String> NO_MATCH = (Map) ImmutableMap.builder()
                                                          .put(LogRollMode.ALPHABETICAL, name)
                                                          .put(LogRollMode.REVERSE_COUNTER, name + ".124x")
                                                          .put(LogRollMode.DATE_YYYY_MM, name + ".2015-13")
                                                          .put(LogRollMode.DATE_YYYY_MM_DD, name + ".2015-12-01x")
                                                          .put(LogRollMode.DATE_YYYY_MM_DD_HH, name + ".2015-12-x1-23")
                                                          .put(LogRollMode.DATE_YYYY_MM_DD_HH_MM, name + ".2015-2-01-23-59")
                                                          .put(LogRollMode.DATE_YYYY_WW, name + "2015-40")
                                                          .build();

    for (Map.Entry<LogRollMode, String> entry : MATCH.entrySet()) {
      Path path = new File(entry.getValue()).toPath();
      PathMatcher fileMatcher = FileSystems.getDefault().getPathMatcher(entry.getKey().getPattern(name));
      Assert.assertTrue(fileMatcher.matches(path));
    }

    for (Map.Entry<LogRollMode, String> entry : NO_MATCH.entrySet()) {
      Path path = new File(entry.getValue()).toPath();
      PathMatcher fileMatcher = FileSystems.getDefault().getPathMatcher(entry.getKey().getPattern(name));
      Assert.assertFalse(fileMatcher.matches(path));
    }

  }

}
