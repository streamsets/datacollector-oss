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

import com.streamsets.datacollector.util.Configuration;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.servlet.ServletProperties;

import java.io.File;

public class DisconnectedSSOManager {
  public static final String SECURITY_RESOURCES = "/security/public-rest/*";

  public static final String DISCONNECTED_SSO_AUTHENTICATION_FILE = "disconnected-sso-credentials.json";

  public static final String DISCONNECTED_SSO_AUTHENTICATION_HANDLER_ATTR = "disconnected.sso.authentication.handler";

  public static final String DISCONNECTED_SSO_SERVICE_ATTR = "disconnected.sso.service";

  private final DisconnectedAuthentication authentication;
  private final DisconnectedSSOService ssoService;

  public DisconnectedSSOManager(String dataDir, Configuration configuration) {
    authentication = new DisconnectedAuthentication(new File(dataDir, DISCONNECTED_SSO_AUTHENTICATION_FILE));
    ssoService = new DisconnectedSSOService(authentication);
    ssoService.setConfiguration(configuration);
  }

  public DisconnectedAuthentication getAuthentication() {
    return authentication;
  }

  public DisconnectedSSOService getSsoService() {
    return ssoService;
  }

  public void setEnabled(boolean enabled) {
    if (enabled && !ssoService.isEnabled()) {
      authentication.reset();
    }
    ssoService.setEnabled(enabled);
  }

  public boolean isEnabled() {
    return ssoService.isEnabled();
  }

  public void registerResources(ServletContextHandler handler) {
    ServletHolder jerseyServlet = new ServletHolder(ServletContainer.class);
    jerseyServlet.setInitParameter(ServerProperties.PROVIDER_PACKAGES, getClass().getPackage().getName());
    jerseyServlet.setInitParameter(
        ServletProperties.JAXRS_APPLICATION_CLASS,
        DisconnectedResourceConfig.class.getName()
    );
    handler.addServlet(jerseyServlet, SECURITY_RESOURCES);

    AuthenticationResourceHandler authenticationResourceHandler =
        new AuthenticationResourceHandler(getAuthentication(), false);

    handler.setAttribute(DISCONNECTED_SSO_AUTHENTICATION_HANDLER_ATTR, authenticationResourceHandler);

    handler.setAttribute(DISCONNECTED_SSO_SERVICE_ATTR, getSsoService());

    ServletHolder login = new ServletHolder(new DisconnectedLoginServlet((DisconnectedSSOService) getSsoService()));
    handler.addServlet(login, DisconnectedLoginServlet.URL_PATH);

    ServletHolder logout = new ServletHolder(new DisconnectedLogoutServlet((DisconnectedSSOService) getSsoService()));
    handler.addServlet(logout, DisconnectedLogoutServlet.URL_PATH);
  }

}
