/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.lib.dirspooler.SpoolDirUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestSpoolDirUtil {

  @Test
  public void globPatternValidation() {
    String expectedPath = "/tmp/streamsets/";
    String expectedPathShort = "/tmp/";
    String expectedPathRoot = "/";
    Assert.assertEquals(expectedPath, SpoolDirUtil.truncateGlobPatternDirectory("/tmp/streamsets/*/*/*"));
    Assert.assertEquals(expectedPath, SpoolDirUtil.truncateGlobPatternDirectory("/tmp/streamsets/*/*/*.json"));
    Assert.assertEquals(expectedPath, SpoolDirUtil.truncateGlobPatternDirectory("/tmp/streamsets/employee*/*/*"));
    Assert.assertEquals(expectedPath, SpoolDirUtil.truncateGlobPatternDirectory("/tmp/streamsets/*/*/*"));
    Assert.assertEquals(expectedPath, SpoolDirUtil.truncateGlobPatternDirectory("/tmp/streamsets/employee*"));
    Assert.assertEquals(expectedPathShort, SpoolDirUtil.truncateGlobPatternDirectory("/tmp/streams*/employee*"));
    Assert.assertEquals(expectedPathRoot, SpoolDirUtil.truncateGlobPatternDirectory("/tmp*"));
    Assert.assertEquals(expectedPathRoot, SpoolDirUtil.truncateGlobPatternDirectory("/*"));
  }

}
