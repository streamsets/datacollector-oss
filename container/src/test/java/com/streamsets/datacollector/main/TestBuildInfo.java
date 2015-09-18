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
