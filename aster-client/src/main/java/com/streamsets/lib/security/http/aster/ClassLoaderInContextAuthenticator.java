/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.lib.security.http.aster;

import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.server.Authentication;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

/**
 * Authenticator that delegates to the authenticator given in the constructor setting the classloader of the
 * given authenticator as context classloader for each invocation (and restoring the original classloader afterwards.
 */
public class ClassLoaderInContextAuthenticator implements Authenticator {
  private final AsterAuthenticator authenticator;

  public ClassLoaderInContextAuthenticator(AsterAuthenticator authenticator) {
    this.authenticator = authenticator;
  }

  public Authenticator getDelegateAuthenticator() {
    return authenticator;
  }

  @Override
  public void setConfiguration(AuthConfiguration authConfiguration) {
    ClassLoaderUtils.withOwnClassLoader(
        authenticator.getClass().getClassLoader(),
        () -> {
          authenticator.setConfiguration(authConfiguration);
          return null;
        }
    );
  }

  @Override
  public String getAuthMethod() {
    return ClassLoaderUtils.withOwnClassLoader(authenticator.getClass().getClassLoader(), authenticator::getAuthMethod);
  }

  @Override
  public void prepareRequest(ServletRequest request) {
    ClassLoaderUtils.withOwnClassLoader(
        authenticator.getClass().getClassLoader(),
        () -> {
          authenticator.prepareRequest(request);
          return null;
        }
    );
  }

  @Override
  public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory) throws
      ServerAuthException {
    return ClassLoaderUtils.withOwnClassLoader(
        authenticator.getClass().getClassLoader(),
        () -> authenticator.validateRequest(request, response, mandatory)
    );
  }

  void handleRegistration(ServletRequest request, ServletResponse response) {
    ClassLoaderUtils.withOwnClassLoader(
        authenticator.getClass().getClassLoader(),
        () -> {
          authenticator.handleRegistration(request, response);
          return null;
        }
    );
  }

  @Override
  public boolean secureResponse(
      ServletRequest request, ServletResponse response, boolean mandatory, Authentication.User validatedUser
  ) throws ServerAuthException {
    return ClassLoaderUtils.withOwnClassLoader(
        authenticator.getClass().getClassLoader(),
        () -> authenticator.secureResponse(request, response, mandatory, validatedUser)
    );
  }
}
