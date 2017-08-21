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
package com.streamsets.lib.security.http;

import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestDisconnectedSecurityInfo {

  @Test
  public void testEntry() {
    DisconnectedSecurityInfo.Entry entry = new DisconnectedSecurityInfo.Entry();
    entry.setUserNameSha("us");
    entry.setPasswordHash("ph");
    entry.getRoles().add("r");
    Assert.assertEquals("us", entry.getUserNameSha());
    Assert.assertEquals("ph", entry.getPasswordHash());
    Assert.assertEquals(ImmutableList.of("r"), entry.getRoles());
  }

  @Test
  public void testInfoMethodsForJson() {
    DisconnectedSecurityInfo info = new DisconnectedSecurityInfo();
    Assert.assertTrue(info.getEntries().isEmpty());

    DisconnectedSecurityInfo.Entry entry = new DisconnectedSecurityInfo.Entry();
    entry.setUserNameSha(DigestUtils.sha256Hex("us"));
    entry.setPasswordHash("ph");
    entry.getRoles().add("r");
    info.setEntries(ImmutableList.of(entry));
    Assert.assertEquals(entry, info.getEntry("us"));
    Assert.assertEquals(ImmutableList.of(entry), info.getEntries());

    entry = new DisconnectedSecurityInfo.Entry();
    entry.setUserNameSha(DigestUtils.sha256Hex("us1"));
    entry.setPasswordHash("ph1");
    entry.getRoles().add("r1");
    info.setEntries(ImmutableList.of(entry));
    Assert.assertEquals(ImmutableList.of(entry), info.getEntries());
  }

  @Test
  public void testInfoPopulation() {
    DisconnectedSecurityInfo info = new DisconnectedSecurityInfo();
    info.addEntry("user", "ph", ImmutableList.of("r"), ImmutableList.of("g"));
    String userSha = DigestUtils.sha256Hex("user");
    DisconnectedSecurityInfo.Entry entry = info.getEntry("user");
    Assert.assertEquals(userSha, entry.getUserNameSha());
    Assert.assertEquals("ph", entry.getPasswordHash());
    Assert.assertEquals(ImmutableList.of("r"), entry.getRoles());
  }

  @Test
  public void testJson() throws Exception {
    DisconnectedSecurityInfo info = new DisconnectedSecurityInfo();
    info.addEntry("user", "ph", ImmutableList.of("r"), ImmutableList.of("g"));
    String json = info.toJsonString();

    info = DisconnectedSecurityInfo.fromJsonString(json);
    Assert.assertNotNull(info.getEntry("user"));
  }

}
