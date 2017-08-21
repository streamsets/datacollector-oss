/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    RollMode rollMode = LogRollModeFactory.ALPHABETICAL.get("foo", "");
    Assert.assertEquals("foo", rollMode.getLiveFileName());

    rollMode = LogRollModeFactory.ALPHABETICAL.get("x", "");
    Assert.assertFalse(rollMode.isFirstAcceptable("x"));
    Assert.assertFalse(rollMode.isFirstAcceptable("xx"));
    Assert.assertTrue(rollMode.isFirstAcceptable(""));
    Assert.assertTrue(rollMode.isFirstAcceptable(null));
    Assert.assertTrue(rollMode.isFirstAcceptable("x.1"));
    Assert.assertFalse(rollMode.isCurrentAcceptable("x"));
    Assert.assertTrue(rollMode.isCurrentAcceptable("y"));
    Assert.assertTrue(rollMode.isCurrentAcceptable(null));

    rollMode = LogRollModeFactory.ALPHABETICAL.get(f1.getFileName().toString(), "");
    Assert.assertFalse(rollMode.isFileRolled(new LiveFile(f1)));

    rollMode = LogRollModeFactory.ALPHABETICAL.get(f2.getFileName().toString(), "");
    Assert.assertTrue(rollMode.isFileRolled(new LiveFile(f1)));

    rollMode = LogRollModeFactory.ALPHABETICAL.get("x", "");
    //for ALPHABETICAL and all DATE_...
    Assert.assertTrue(rollMode.getComparator().compare(f1, f2) < 0);

    //for REVERSE_COUNTER
    Assert.assertTrue(rollMode.getComparator().compare(f1, f2) < 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRolledFilesModePatterns() throws Exception {
    String name = new File("target/" + UUID.randomUUID().toString(), "my.log").getAbsolutePath();

    Map<LogRollModeFactory, String> MATCH = (Map) ImmutableMap.builder()
                                                       .put(LogRollModeFactory.ALPHABETICAL, name + ".a")
                                                       .put(LogRollModeFactory.REVERSE_COUNTER, name + ".124")
                                                       .put(LogRollModeFactory.DATE_YYYY_MM, name + ".2015-12")
                                                       .put(LogRollModeFactory.DATE_YYYY_MM_DD, name + ".2015-12-01")
                                                       .put(LogRollModeFactory.DATE_YYYY_MM_DD_HH, name + ".2015-12-01-23")
                                                       .put(LogRollModeFactory.DATE_YYYY_MM_DD_HH_MM, name + ".2015-12-01-23-59")
                                                       .put(LogRollModeFactory.DATE_YYYY_WW, name + ".2015-40")
                                                       .build();

    Map<LogRollModeFactory, String> NO_MATCH = (Map) ImmutableMap.builder()
                                                          .put(LogRollModeFactory.ALPHABETICAL, name)
                                                          .put(LogRollModeFactory.REVERSE_COUNTER, name + ".124x")
                                                          .put(LogRollModeFactory.DATE_YYYY_MM, name + ".2015-13")
                                                          .put(LogRollModeFactory.DATE_YYYY_MM_DD, name + ".2015-12-01x")
                                                          .put(LogRollModeFactory.DATE_YYYY_MM_DD_HH, name + ".2015-12-x1-23")
                                                          .put(LogRollModeFactory.DATE_YYYY_MM_DD_HH_MM, name + ".2015-2-01-23-59")
                                                          .put(LogRollModeFactory.DATE_YYYY_WW, name + "2015-40")
                                                          .build();

    for (Map.Entry<LogRollModeFactory, String> entry : MATCH.entrySet()) {
      Path path = new File(entry.getValue()).toPath();
      PathMatcher fileMatcher = FileSystems.getDefault().getPathMatcher(entry.getKey().get(name, "").getPattern());
      Assert.assertTrue(fileMatcher.matches(path));
    }

    for (Map.Entry<LogRollModeFactory, String> entry : NO_MATCH.entrySet()) {
      Path path = new File(entry.getValue()).toPath();
      PathMatcher fileMatcher = FileSystems.getDefault().getPathMatcher(entry.getKey().get(name, "").getPattern());
      Assert.assertFalse(fileMatcher.matches(path));
    }

  }

}
