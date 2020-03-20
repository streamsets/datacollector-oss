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

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.UserIdentity;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.security.Principal;
import java.util.Set;

/**
 * Jetty Authenticator proxy that performs activation checks.
 * <p/>
 * If activation is enabled all users have GUEST role only.
 */
public class ActivationAuthenticator implements Authenticator {
  private final Authenticator authenticator;
  private final Activation activation;
  static final Set<String> TRIAL_ALLOWED_ROLES = ImmutableSet.of(
      "user",
      AuthzRole.GUEST,
      AuthzRole.GUEST_REMOTE
  );
  static final Set<String> NO_TRIAL_ALLOWED_ROLES = ImmutableSet.of("user");


  public ActivationAuthenticator(Authenticator authenticator, Activation activation) {
    Utils.checkNotNull(authenticator, "authenticator");
    Utils.checkNotNull(activation, "activation");
    this.authenticator = authenticator;
    this.activation = activation;
  }

  @Override
  public void setConfiguration(AuthConfiguration configuration) {
    authenticator.setConfiguration(configuration);
  }

  @Override
  public String getAuthMethod() {
    return authenticator.getAuthMethod();
  }

  @Override
  public void prepareRequest(ServletRequest request) {
    authenticator.prepareRequest(request);
  }

  @Override
  public Authentication validateRequest(
      ServletRequest request, ServletResponse response, boolean mandatory
  ) throws ServerAuthException {
    Authentication authentication = authenticator.validateRequest(request, response, mandatory);
    if (authentication instanceof Authentication.User) {
      Activation.Info activationInfo = activation.getInfo();
      if (activation.isEnabled() && !activationInfo.isValid()) {
        boolean hasTrial = activationInfo.getExpiration() > 0;
        authentication = new ExpiredActivationUser(
            (Authentication.User) authentication,
            hasTrial ? TRIAL_ALLOWED_ROLES : NO_TRIAL_ALLOWED_ROLES
        );
      }
    }
    return authentication;
  }

  @Override
  public boolean secureResponse(
      ServletRequest request, ServletResponse response, boolean mandatory, Authentication.User validatedUser
  ) throws ServerAuthException {
    return authenticator.secureResponse(request, response, mandatory, validatedUser);
  }

  static class ExpiredActivationUser implements Authentication.User {

    private final Authentication.User user;
    private final Set<String> allowedRoles;

    public ExpiredActivationUser(User user, Set<String> allowedRoles) {
      this.user = user;
      this.allowedRoles = allowedRoles;
    }

    @Override
    public String getAuthMethod() {
      return user.getAuthMethod();
    }

    @Override
    public UserIdentity getUserIdentity() {
      final UserIdentity userIdentity = user.getUserIdentity();
      return new UserIdentity() {
        @Override
        public Subject getSubject() {
          return userIdentity.getSubject();
        }

        @Override
        public Principal getUserPrincipal() {
          return userIdentity.getUserPrincipal();
        }

        @Override
        public boolean isUserInRole(String role, Scope scope) {
          return ExpiredActivationUser.this.isUserInRole(scope, role);
        }
      };
    }

    @Override
    public boolean isUserInRole(UserIdentity.Scope scope, String role) {
      if (allowedRoles.contains(role)) {
        return true;
      } else if (AuthzRole.ADMIN_ACTIVATION.equals(role) &&
                 (user.isUserInRole(scope, AuthzRole.ADMIN) ||
                  (user.isUserInRole(scope, AuthzRole.ADMIN_REMOTE)))) {
        return true;
      }
      return false;
    }

    @Override
    public void logout() {
      logout(null);
    }

    @Override
    public Authentication logout(ServletRequest request) {
      return user.logout(request);
    }
  }

}
