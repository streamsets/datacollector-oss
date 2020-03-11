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
package com.streamsets.datacollector.security;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;

public class TestGroupsInScope {

  @Test
  public void testExecute() throws Exception {
    Callable<Boolean> callable = () -> GroupsInScope.isUserGroupInScope("g");
    Assert.assertTrue(GroupsInScope.execute(ImmutableSet.of("g"), callable));
    Assert.assertFalse(GroupsInScope.execute(ImmutableSet.of("x"), callable));
  }

  @Test
  public void testExecuteIgnoreGroups() throws Exception {
    Callable<Boolean> callable = () -> GroupsInScope.isUserGroupInScope("g");
    Assert.assertTrue(GroupsInScope.executeIgnoreGroups(callable));
  }

  @Test(expected = IllegalStateException.class)
  public void testOutOfScope() throws Exception {
    GroupsInScope.isUserGroupInScope("g");
  }


  @Test
  public void testAllGroup() throws Exception {
    GroupsInScope.execute(ImmutableSet.of(), () -> {
      Assert.assertTrue(GroupsInScope.isUserGroupInScope("all"));
      return null;
    });
  }

}
