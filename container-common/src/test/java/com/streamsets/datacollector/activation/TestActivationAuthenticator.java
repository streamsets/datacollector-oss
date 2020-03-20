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
package com.streamsets.datacollector.activation;

import com.streamsets.datacollector.util.AuthzRole;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.UserIdentity;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.security.Principal;

public class TestActivationAuthenticator {

  @Test
  public void testCleanDelegationMethods() throws Exception {
    Authenticator auth = Mockito.mock(Authenticator.class);
    Activation activation = Mockito.mock(Activation.class);
    ActivationAuthenticator activationAuth = new ActivationAuthenticator(auth, activation);

    Authenticator.AuthConfiguration conf = Mockito.mock(Authenticator.AuthConfiguration.class);
    activationAuth.setConfiguration(conf);
    Mockito.verify(auth, Mockito.times(1)).setConfiguration(Mockito.eq(conf));

    Mockito.when(auth.getAuthMethod()).thenReturn("foo");
    Assert.assertEquals("foo", activationAuth.getAuthMethod());

    ServletRequest req = Mockito.mock(ServletRequest.class);
    activationAuth.prepareRequest(req);
    Mockito.verify(auth, Mockito.times(1)).prepareRequest(Mockito.eq(req));

    ServletResponse res = Mockito.mock(ServletResponse.class);
    Authentication.User user = Mockito.mock(Authentication.User.class);
    Mockito.when(auth.secureResponse(Mockito.eq(req), Mockito.eq(res), Mockito.eq(true), Mockito.eq(user)))
           .thenReturn(true);
    Assert.assertTrue(auth.secureResponse(req, res, true, user));
  }

  @Test
  public void testValidateRequestDelegationNotEnabled() throws Exception {
    Authenticator auth = Mockito.mock(Authenticator.class);
    Activation activation = Mockito.mock(Activation.class);
    ActivationAuthenticator activationAuth = new ActivationAuthenticator(auth, activation);

    ServletRequest req = Mockito.mock(ServletRequest.class);
    ServletResponse res = Mockito.mock(ServletResponse.class);
    Authentication authResponse = Mockito.mock(Authentication.class);

    Mockito.when(auth.validateRequest(Mockito.eq(req), Mockito.eq(res), Mockito.eq(false))).thenReturn(authResponse);

    // test not user, activation not enabled
    Mockito.when(activation.isEnabled()).thenReturn(false);
    Assert.assertEquals(authResponse, activationAuth.validateRequest(req, res, false));

    // test not user, activation enabled
    Mockito.when(activation.isEnabled()).thenReturn(true);
    Assert.assertEquals(authResponse, activationAuth.validateRequest(req, res, false));

    // test user, activation not enabled
    authResponse = Mockito.mock(Authentication.User.class);
    Mockito.when(auth.validateRequest(Mockito.eq(req), Mockito.eq(res), Mockito.eq(false))).thenReturn(authResponse);
    Mockito.when(activation.isEnabled()).thenReturn(false);
    Assert.assertEquals(authResponse, activationAuth.validateRequest(req, res, false));

    // test user, activation enabled, activation not expired
    Mockito.when(activation.isEnabled()).thenReturn(true);
    Activation.Info info = Mockito.mock(Activation.Info.class);
    Mockito.when(info.isValid()).thenReturn(true);
    Mockito.when(activation.getInfo()).thenReturn(info);
    Assert.assertEquals(authResponse, activationAuth.validateRequest(req, res, false));

    // test user, activation enabled, activation expired
    Mockito.when(info.isValid()).thenReturn(false);
    Authentication authResponseGot = activationAuth.validateRequest(req, res, false);
    Assert.assertTrue(authResponseGot instanceof ActivationAuthenticator.ExpiredActivationUser);
  }

  @Test
  public void testExpiredActivationUserWithTrial() {
    Authentication.User authUser = Mockito.mock(Authentication.User.class);
    Subject subject = new Subject();
    Principal principal = Mockito.mock(Principal.class);
    UserIdentity userIdentity = Mockito.mock(UserIdentity.class);
    Mockito.when(userIdentity.getSubject()).thenReturn(subject);
    Mockito.when(userIdentity.getUserPrincipal()).thenReturn(principal);
    Mockito.when(authUser.getUserIdentity()).thenReturn(userIdentity);
    Authentication.User expiredAuthUser = new ActivationAuthenticator.ExpiredActivationUser(
        authUser,
        ActivationAuthenticator.TRIAL_ALLOWED_ROLES
    );

    Mockito.when(authUser.getAuthMethod()).thenReturn("foo");
    Assert.assertEquals("foo", expiredAuthUser.getAuthMethod());
    Mockito.verify(authUser, Mockito.times(1)).getAuthMethod();

    // Call to expiredAuthUser calls Jetty logout implementation that takes (null) request
    expiredAuthUser.logout();
    Mockito.verify(authUser, Mockito.times(1)).logout(null);

    // non admin user
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, "user"));
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST));
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST_REMOTE));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN_ACTIVATION));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, "foo"));

    // admin user
    Mockito.when(authUser.isUserInRole(Mockito.eq(null), Mockito.eq(AuthzRole.ADMIN))).thenReturn(true);
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, "user"));
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST));
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST_REMOTE));
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN_ACTIVATION));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN_REMOTE));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, "foo"));

    // remote admin user
    Mockito.when(authUser.isUserInRole(Mockito.eq(null), Mockito.eq(AuthzRole.ADMIN_REMOTE))).thenReturn(true);
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, "user"));
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST));
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST_REMOTE));
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN_ACTIVATION));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN_REMOTE));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, "foo"));

    // verify UserIdentity.isUserInRole() delegation to ExpiredActivationUser.isUserInRole()
    expiredAuthUser = new ActivationAuthenticator.ExpiredActivationUser(
        authUser,
        ActivationAuthenticator.TRIAL_ALLOWED_ROLES
    );
    expiredAuthUser = Mockito.spy(expiredAuthUser);
    userIdentity = expiredAuthUser.getUserIdentity();
    UserIdentity.Scope scope = Mockito.mock(UserIdentity.Scope.class);
    Assert.assertTrue(userIdentity.isUserInRole(AuthzRole.GUEST,scope));
    Mockito.verify(expiredAuthUser, Mockito.times(1)).isUserInRole(Mockito.eq(scope), Mockito.eq(AuthzRole.GUEST));
  }

  @Test
  public void testExpiredActivationUserWithNoTrial() {
    Authentication.User authUser = Mockito.mock(Authentication.User.class);
    Subject subject = new Subject();
    Principal principal = Mockito.mock(Principal.class);
    UserIdentity userIdentity = Mockito.mock(UserIdentity.class);
    Mockito.when(userIdentity.getSubject()).thenReturn(subject);
    Mockito.when(userIdentity.getUserPrincipal()).thenReturn(principal);
    Mockito.when(authUser.getUserIdentity()).thenReturn(userIdentity);
    Authentication.User expiredAuthUser = new ActivationAuthenticator.ExpiredActivationUser(
        authUser,
        ActivationAuthenticator.NO_TRIAL_ALLOWED_ROLES
    );

    Mockito.when(authUser.getAuthMethod()).thenReturn("foo");
    Assert.assertEquals("foo", expiredAuthUser.getAuthMethod());
    Mockito.verify(authUser, Mockito.times(1)).getAuthMethod();

    // Call to expiredAuthUser calls Jetty logout implementation that takes (null) request
    expiredAuthUser.logout();
    Mockito.verify(authUser, Mockito.times(1)).logout(null);

    // non admin user
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, "user"));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST_REMOTE));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN_ACTIVATION));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, "foo"));

    // admin user
    Mockito.when(authUser.isUserInRole(Mockito.eq(null), Mockito.eq(AuthzRole.ADMIN))).thenReturn(true);
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, "user"));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST_REMOTE));
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN_ACTIVATION));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN_REMOTE));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, "foo"));

    // remote admin user
    Mockito.when(authUser.isUserInRole(Mockito.eq(null), Mockito.eq(AuthzRole.ADMIN_REMOTE))).thenReturn(true);
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, "user"));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.GUEST_REMOTE));
    Assert.assertTrue(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN_ACTIVATION));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, AuthzRole.ADMIN_REMOTE));
    Assert.assertFalse(expiredAuthUser.isUserInRole(null, "foo"));

    // verify UserIdentity.isUserInRole() delegation to ExpiredActivationUser.isUserInRole()
    expiredAuthUser = new ActivationAuthenticator.ExpiredActivationUser(
        authUser,
        ActivationAuthenticator.NO_TRIAL_ALLOWED_ROLES
    );
    expiredAuthUser = Mockito.spy(expiredAuthUser);
    userIdentity = expiredAuthUser.getUserIdentity();
    UserIdentity.Scope scope = Mockito.mock(UserIdentity.Scope.class);
    Assert.assertFalse(userIdentity.isUserInRole(AuthzRole.GUEST,scope));
    Mockito.verify(expiredAuthUser, Mockito.times(1)).isUserInRole(Mockito.eq(scope), Mockito.eq(AuthzRole.GUEST));
  }
}
