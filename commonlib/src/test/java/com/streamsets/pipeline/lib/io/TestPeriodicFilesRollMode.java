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

    RollMode rollMode = new PeriodicFilesRollModeFactory().get("x${PATTERN}x", ".");
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
