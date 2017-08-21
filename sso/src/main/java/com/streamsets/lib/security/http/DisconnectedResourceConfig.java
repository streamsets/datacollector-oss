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

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

public class DisconnectedResourceConfig extends ResourceConfig {

  public static class DisconnectedSSOServiceInjector implements Factory<DisconnectedSSOService> {
    private final DisconnectedSSOService service;

    @Inject
    public DisconnectedSSOServiceInjector(HttpServletRequest request) {
      service = (DisconnectedSSOService) request
          .getServletContext()
          .getAttribute(DisconnectedSSOManager.DISCONNECTED_SSO_SERVICE_ATTR);
    }

    @Override
    public DisconnectedSSOService provide() {
      return service;
    }

    @Override
    public void dispose(DisconnectedSSOService ssoService) {
    }
  }

  public static class AuthenticationResourceHandlerInjector implements Factory<AuthenticationResourceHandler> {
    private final AuthenticationResourceHandler handler;

    @Inject
    public AuthenticationResourceHandlerInjector(HttpServletRequest request) {
      handler = (AuthenticationResourceHandler) request
          .getServletContext()
          .getAttribute(DisconnectedSSOManager.DISCONNECTED_SSO_AUTHENTICATION_HANDLER_ATTR);
    }

    @Override
    public AuthenticationResourceHandler provide() {
      return handler;
    }

    @Override
    public void dispose(AuthenticationResourceHandler handler) {
    }
  }

  public DisconnectedResourceConfig() {
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bindFactory(DisconnectedSSOServiceInjector.class).to(DisconnectedSSOService.class);
        bindFactory(AuthenticationResourceHandlerInjector.class).to(AuthenticationResourceHandler.class);
      }
    });
  }

}
