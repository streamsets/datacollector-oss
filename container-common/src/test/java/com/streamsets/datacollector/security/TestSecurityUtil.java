/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.security;

import org.junit.Assert;
import org.junit.Test;

import javax.security.auth.Subject;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

public class TestSecurityUtil {

  @Test
  public void testdoAs() throws Exception {
    PrivilegedExceptionAction<Void> exAction = () -> {
      if (SecurityUtil.getJavaVersion() <= 9) {
        Assert.assertTrue(AccessController.getContext().getDomainCombiner() instanceof SecurityUtil.CustomCombiner);
      }
      return null;
    };
    SecurityUtil.doAs(new Subject(), exAction);

    PrivilegedAction<Void> action = () -> {
      if (SecurityUtil.getJavaVersion() <= 9) {
        Assert.assertTrue(AccessController.getContext().getDomainCombiner() instanceof SecurityUtil.CustomCombiner);
      }
      return null;
    };
    SecurityUtil.doAs(new Subject(), action);
  }
}
