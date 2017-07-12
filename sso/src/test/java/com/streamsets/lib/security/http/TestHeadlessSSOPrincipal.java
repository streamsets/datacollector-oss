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

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

public class TestHeadlessSSOPrincipal {

  @Test
  public void testRecoveryPrincipal() {
    HeadlessSSOPrincipal principal = HeadlessSSOPrincipal.createRecoveryPrincipal("id");
    Assert.assertEquals("id", principal.getName());
    Assert.assertTrue(principal.getGroups().isEmpty());
    Assert.assertTrue(principal.isRecovery());
  }

  @Test
  public void testConstructorPrincipal() {
    HeadlessSSOPrincipal principal = new HeadlessSSOPrincipal("id", ImmutableSet.of("g"));
    Assert.assertEquals("id", principal.getName());
    Assert.assertEquals(ImmutableSet.of("g"), principal.getGroups());
    Assert.assertFalse(principal.isRecovery());
  }

}
