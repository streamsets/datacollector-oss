/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.main;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import com.streamsets.datacollector.main.BuildInfo;

public class TestBuildInfo {

  @Test
  public void testInfo() {
    BuildInfo info = new BuildInfo();
    Assert.assertEquals("1", info.getVersion());
    Assert.assertEquals("today", info.getBuiltDate());
    Assert.assertEquals("foo", info.getBuiltBy());
    Assert.assertEquals("sha", info.getBuiltRepoSha());
    Assert.assertEquals("container-checksum", info.getImplSourceMd5Checksum());
    Assert.assertEquals("api-checksum", info.getApiSourceMd5Checksum());
    Logger log = Mockito.mock(Logger.class);
    info.log(log);
  }
}
