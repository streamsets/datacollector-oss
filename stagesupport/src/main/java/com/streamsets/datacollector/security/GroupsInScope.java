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

import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class GroupsInScope {
  private static final Set<String> BYPASS_GROUPS = new HashSet<>();
  private static final String ALL_GROUP = "all";
  private static final ThreadLocal<Set<String>> GROUPS_IN_SCOPE_TL = new ThreadLocal<>();


  public static <T> T execute(Set<String> userGroups, Callable<T> callable) throws Exception {
    Utils.checkNotNull(callable, "callable");
    try {
      GROUPS_IN_SCOPE_TL.set(userGroups);
      return callable.call();
    } finally {
      GROUPS_IN_SCOPE_TL.remove();
    }
  }

  public static <T> T executeIgnoreGroups(Callable<T> callable) throws Exception {
    return execute(BYPASS_GROUPS, callable);
  }

  public static Set<String> getUserGroupsInScope() {
    return GROUPS_IN_SCOPE_TL.get();

  }
  public static boolean isUserGroupInScope(String userGroup) {
    Utils.checkNotNull(userGroup, "userGroup");
    Utils.checkState(GROUPS_IN_SCOPE_TL.get() != null, "Not in UserGroupScope scope");
    return (GROUPS_IN_SCOPE_TL.get() == BYPASS_GROUPS) || userGroup.equals(ALL_GROUP) || GROUPS_IN_SCOPE_TL.get().contains(userGroup);
  }

}
