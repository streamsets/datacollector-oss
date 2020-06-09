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
package com.streamsets.datacollector.main;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class TestBuildInfo {

  @Test
  public void testInfo() {
    BuildInfo info = new BuildInfo("test-buildinfo.properties"){};
    Assert.assertEquals("1", info.getVersion());
    Assert.assertEquals("today", info.getBuiltDate());
    Assert.assertEquals("foo", info.getBuiltBy());
    Assert.assertEquals("sha", info.getBuiltRepoSha());
    Assert.assertEquals("checksum", info.getSourceMd5Checksum());
    Assert.assertEquals("1", info.getInfo().getProperty("version"));
    Assert.assertEquals(5, info.getInfo().size());
    Logger log = Mockito.mock(Logger.class);
    info.log(log);
  }
}
