/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.lib.security.http;

import org.junit.Assert;
import org.junit.Test;

public class TestSSOAuthenticationUser {

  @Test
  public void testUser() {
    SSOUserPrincipal principal = TestSSOUserPrincipalImpl.createPrincipal();
    SSOAuthenticationUser user = new SSOAuthenticationUser(principal);
    Assert.assertEquals(SSOConstants.AUTHENTICATION_METHOD, user.getAuthMethod());
    Assert.assertEquals(principal, user.getToken());
    Assert.assertEquals(principal.getName(), user.getUserIdentity().getUserPrincipal().getName());
    Assert.assertEquals(1, user.getUserIdentity().getSubject().getPrincipals().size());
    Assert.assertEquals(
        user.getUserIdentity().getUserPrincipal(),
        user.getUserIdentity().getSubject().getPrincipals().iterator().next()
    );
    Assert.assertTrue(user.getUserIdentity().isUserInRole("ROLE1", null));
    Assert.assertFalse(user.getUserIdentity().isUserInRole("NOT_THERE", null));

    Assert.assertTrue(user.isValid());

    user.logout();

    Assert.assertFalse(user.isValid());

    Assert.assertEquals(principal, user.getUserIdentity().getUserPrincipal());
  }


}
