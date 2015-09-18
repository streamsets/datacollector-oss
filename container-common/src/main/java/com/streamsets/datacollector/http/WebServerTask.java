/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.datacollector.http;

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;

import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.security.authentication.DigestAuthenticator;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.session.HashSessionManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.Set;

public class WebServerTask extends AbstractTask {
  public static final String HTTP_PORT_KEY = "http.port";
  private static final int HTTP_PORT_DEFAULT = 0;

  public static final String HTTPS_PORT_KEY = "https.port";
  private static final int HTTPS_PORT_DEFAULT = -1;
  public static final String HTTPS_KEYSTORE_PATH_KEY = "https.keystore.path";
  private static final String HTTPS_KEYSTORE_PATH_DEFAULT = "sdc-keystore.jks";
  public static final String HTTPS_KEYSTORE_PASSWORD_KEY = "https.keystore.password";
  private static final String HTTPS_KEYSTORE_PASSWORD_DEFAULT = "@sdc-keystore-password.txt@";

  public static final String AUTHENTICATION_KEY = "http.authentication";
  public static final String AUTHENTICATION_DEFAULT = "none"; //"form";

  private static final String DIGEST_REALM_KEY = "http.digest.realm";
  private static final String REALM_POSIX_DEFAULT = "-realm";

  private static final String REALM_FILE_PERMISSION_CHECK = "http.realm.file.permission.check";
  private static final boolean REALM_FILE_PERMISSION_CHECK_DEFAULT = true;

  private static final String JSESSIONID_COOKIE = "JSESSIONID_";

  private static final Set<String> AUTHENTICATION_MODES = ImmutableSet.of("none", "digest", "basic", "form");

  private static final Logger LOG = LoggerFactory.getLogger(WebServerTask.class);

  private final RuntimeInfo runtimeInfo;
  private final Configuration conf;
  private final Set<ContextConfigurator> contextConfigurators;
  private int port;
  private Server server;
  private Server redirector;
  private HashSessionManager hashSessionManager;

  @Inject
  public WebServerTask(RuntimeInfo runtimeInfo, Configuration conf, Set<ContextConfigurator> contextConfigurators) {
    super("webServer");
    this.runtimeInfo = runtimeInfo;
    this.conf = conf;
    this.contextConfigurators = contextConfigurators;
  }

  @Override
  public void initTask() {
    checkValidPorts();
    server = createServer();
    int port = -1;
    ServletContextHandler appHandler = configureAppContext();
    Handler handler = configureAuthentication(server, appHandler);
    handler = configureRedirectionRules(handler);
    server.setHandler(handler);
    if (isRedirectorToSSLEnabled()) {
      redirector = createRedirectorServer();
    }
  }


  private ServletContextHandler configureAppContext() {
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);

    hashSessionManager = new HashSessionManager();
    context.setSessionHandler(new SessionHandler(hashSessionManager));

    context.setContextPath("/");
    for (ContextConfigurator cc : contextConfigurators) {
      cc.init(context);
    }
    return context;
  }

  private Handler configureRedirectionRules(Handler appHandler) {
    RewriteHandler handler = new RewriteHandler();
    handler.setRewriteRequestURI(false);
    handler.setRewritePathInfo(false);
    handler.setOriginalPathAttribute("requestedPath");

    RewriteRegexRule uiRewriteRule = new RewriteRegexRule();
    uiRewriteRule.setRegex("^/collector/.*");
    uiRewriteRule.setReplacement("/");
    handler.addRule(uiRewriteRule);
    handler.setHandler(appHandler);

    HandlerCollection handlerCollection = new HandlerCollection();
    handlerCollection.setHandlers(new Handler[] {handler, appHandler});
    return handlerCollection;
  }

  private Handler configureAuthentication(Server server, ServletContextHandler appHandler) {
    String auth = conf.get(AUTHENTICATION_KEY, AUTHENTICATION_DEFAULT);
    switch (auth) {
      case "none":
        break;
      case "digest":
      case "basic":
        appHandler.setSecurityHandler(configureDigestBasic(server, auth));
        break;
      case "form":
        appHandler.setSecurityHandler(configureForm(server));
        break;
      default:
        throw new RuntimeException(Utils.format("Invalid authentication mode '{}', must be one of '{}'",
                                                auth, AUTHENTICATION_MODES));
    }
    return appHandler;
  }

  public static final Set<PosixFilePermission> OWNER_PERMISSIONS = ImmutableSet.of(PosixFilePermission.OWNER_EXECUTE,
                                                                                    PosixFilePermission.OWNER_READ,
                                                                                    PosixFilePermission.OWNER_WRITE);

  private void validateRealmFile(File realmFile) {
    boolean checkRealmFilePermission = conf.get(REALM_FILE_PERMISSION_CHECK, REALM_FILE_PERMISSION_CHECK_DEFAULT);
    if(!checkRealmFilePermission) {
      return;
    }

    if (!realmFile.exists()) {
      throw new RuntimeException(Utils.format("Realm file '{}' does not exists", realmFile));
    }
    if (!realmFile.isFile()) {
      throw new RuntimeException(Utils.format("Realm file '{}' is not a file", realmFile));
    }
    try {
      Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(realmFile.toPath());
      permissions.removeAll(OWNER_PERMISSIONS);
      if (!permissions.isEmpty()) {
        throw new RuntimeException(Utils.format("The permissions of the realm file '{}' should be owner only",
                                                realmFile));
      }
    } catch (IOException ex) {
      throw new RuntimeException(Utils.format("Could not get the permissions of the realm file '{}', {}", realmFile,
                                              ex.toString(), ex));
    }
  }

  private SecurityHandler configureDigestBasic(Server server, String mode) {
    String realm = conf.get(DIGEST_REALM_KEY, mode + REALM_POSIX_DEFAULT);
    File realmFile = new File(runtimeInfo.getConfigDir(), realm + ".properties").getAbsoluteFile();
    validateRealmFile(realmFile);
    LoginService loginService = new HashLoginService(realm, realmFile.getAbsolutePath());
    server.addBean(loginService);

    ConstraintSecurityHandler security = new ConstraintSecurityHandler();
    Constraint constraint = new Constraint();
    constraint.setName("auth");
    constraint.setAuthenticate(true);
    constraint.setRoles(new String[] { "user"});
    ConstraintMapping mapping = new ConstraintMapping();
    mapping.setPathSpec("/*");
    mapping.setConstraint(constraint);
    security.setConstraintMappings(Collections.singletonList(mapping));
    switch (mode) {
      case "digest":
        security.setAuthenticator(new ProxyAuthenticator(new DigestAuthenticator(), runtimeInfo));
        break;
      case "basic":
        security.setAuthenticator(new ProxyAuthenticator(new BasicAuthenticator(), runtimeInfo));
        break;
    }
    security.setLoginService(loginService);
    return security;
  }

  private SecurityHandler configureForm(Server server) {
    String realm = conf.get(DIGEST_REALM_KEY, "form" + REALM_POSIX_DEFAULT);
    File realmFile = new File(runtimeInfo.getConfigDir(), realm + ".properties").getAbsoluteFile();
    validateRealmFile(realmFile);

    ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();

    Constraint constraint = new Constraint();
    constraint.setName("auth");
    constraint.setAuthenticate(true);
    constraint.setRoles(new String[]{"user"});

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setPathSpec("/*");
    constraintMapping.setConstraint(constraint);

    securityHandler.addConstraintMapping(constraintMapping);

    Constraint noAuthConstraint = new Constraint();
    noAuthConstraint.setName("auth");
    noAuthConstraint.setAuthenticate(false);
    noAuthConstraint.setRoles(new String[]{"user"});


    ConstraintMapping resourceMapping = new ConstraintMapping();
    resourceMapping.setPathSpec("/login");
    resourceMapping.setConstraint(noAuthConstraint);
    securityHandler.addConstraintMapping(resourceMapping);

    resourceMapping = new ConstraintMapping();
    resourceMapping.setPathSpec("/rest/v1/authentication/login");
    resourceMapping.setConstraint(noAuthConstraint);
    securityHandler.addConstraintMapping(resourceMapping);

    // TODO - remove after refactoring
    resourceMapping = new ConstraintMapping();
    resourceMapping.setPathSpec("/rest/v1/cluster/callback");
    resourceMapping.setConstraint(noAuthConstraint);
    securityHandler.addConstraintMapping(resourceMapping);

    resourceMapping = new ConstraintMapping();
    resourceMapping.setPathSpec("/app/*");
    resourceMapping.setConstraint(noAuthConstraint);
    securityHandler.addConstraintMapping(resourceMapping);

    resourceMapping = new ConstraintMapping();
    resourceMapping.setPathSpec("/assets/*");
    resourceMapping.setConstraint(noAuthConstraint);
    securityHandler.addConstraintMapping(resourceMapping);

    resourceMapping = new ConstraintMapping();
    resourceMapping.setPathSpec("/bower_components/*");
    resourceMapping.setConstraint(noAuthConstraint);
    securityHandler.addConstraintMapping(resourceMapping);

    resourceMapping = new ConstraintMapping();
    resourceMapping.setPathSpec("/fonts/*");
    resourceMapping.setConstraint(noAuthConstraint);
    securityHandler.addConstraintMapping(resourceMapping);

    resourceMapping = new ConstraintMapping();
    resourceMapping.setPathSpec("/i18n/*");
    resourceMapping.setConstraint(noAuthConstraint);
    securityHandler.addConstraintMapping(resourceMapping);

    HashLoginService loginService = new HashLoginService(realm, realmFile.getAbsolutePath());
    server.addBean(loginService);
    securityHandler.setLoginService(loginService);

    FormAuthenticator authenticator = new FormAuthenticator("/login.html", "/login.html?error=true", true);
    securityHandler.setAuthenticator(new ProxyAuthenticator(authenticator, runtimeInfo));
    return securityHandler;
  }

  private boolean isSSLEnabled() {
    return conf.get(HTTPS_PORT_KEY, HTTPS_PORT_DEFAULT) != -1;
  }

  // Currently if http or https is random (set to 0), the other should be unused (set to -1)
  // We have this restriction as currently we are not exposing API's for publishing the redirector
  // port.
  private void checkValidPorts() {
    if ((conf.get(HTTP_PORT_KEY, HTTP_PORT_DEFAULT) == 0 && conf.get(HTTPS_PORT_KEY,
      HTTPS_PORT_DEFAULT) != -1)
        || (conf.get(HTTPS_PORT_KEY, HTTPS_PORT_DEFAULT) == 0 && conf.get(HTTP_PORT_KEY,
          HTTP_PORT_DEFAULT) != -1)) {
      throw new IllegalArgumentException(
          "Invalid port combination for http and https, If http port is set to 0 (random), then https should be "
              + "set to -1 or vice versa");
    }
  }

  private boolean isRedirectorToSSLEnabled() {
    return conf.get(HTTPS_PORT_KEY, HTTPS_PORT_DEFAULT) != -1 && conf.get(HTTP_PORT_KEY, HTTP_PORT_DEFAULT) != -1;
  }

  private Server createServer() {
    Server server = new Server();
    if (!isSSLEnabled()) {
      port = conf.get(HTTP_PORT_KEY, HTTP_PORT_DEFAULT);
      return new Server(port);
    } else {
      port = conf.get(HTTPS_PORT_KEY, HTTPS_PORT_DEFAULT);
      File keyStore = new File(runtimeInfo.getConfigDir(),
                               conf.get(HTTPS_KEYSTORE_PATH_KEY, HTTPS_KEYSTORE_PATH_DEFAULT)).getAbsoluteFile();
      if (!keyStore.exists()) {
        throw new RuntimeException(Utils.format("KeyStore file '{}' does not exist", keyStore.getPath()));
      }
      String password = conf.get(HTTPS_KEYSTORE_PASSWORD_KEY, HTTPS_KEYSTORE_PASSWORD_DEFAULT);

      //Create a connector for HTTPS
      HttpConfiguration httpsConf = new HttpConfiguration();
      httpsConf.addCustomizer(new SecureRequestCustomizer());
      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setKeyStorePath(keyStore.getPath());
      sslContextFactory.setKeyStorePassword(password);
      sslContextFactory.setKeyManagerPassword(password);
      ServerConnector httpsConnector = new ServerConnector(server,
                                                           new SslConnectionFactory(sslContextFactory, "http/1.1"),
                                                           new HttpConnectionFactory(httpsConf));
      httpsConnector.setPort(port);
      server.setConnectors(new Connector[]{httpsConnector});
    }
    return server;
  }

  private Server createRedirectorServer() {
    int unsecurePort = conf.get(HTTP_PORT_KEY, HTTP_PORT_DEFAULT);
    Server server = new Server(unsecurePort);
    ServletContextHandler context = new ServletContextHandler();
    context.addServlet(new ServletHolder(new RedirectorServlet()), "/*");
    context.setContextPath("/");
    server.setHandler(context);
    return server;
  }

  private class RedirectorServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      StringBuffer sb = req.getRequestURL();
      String qs = req.getQueryString();
      if (qs != null) {
        sb.append("?").append(qs);
      }
      URL httpUrl = new URL(sb.toString());
      URL httpsUrl = new URL("https", httpUrl.getHost(), port, httpUrl.getFile());
      resp.sendRedirect(httpsUrl.toString());
    }
  }

  @Override
  protected void runTask() {
    for (ContextConfigurator cc : contextConfigurators) {
      cc.start();
    }
    try {
      server.start();
      port = server.getURI().getPort();
      hashSessionManager.setSessionCookie(JSESSIONID_COOKIE + port);
      LOG.info("Running on URI '{}', HTTPS '{}' ",server.getURI(), isSSLEnabled());
      if(runtimeInfo.getBaseHttpUrl().equals(RuntimeInfo.UNDEF)) {
        try {
          String baseHttpUrl = "http://";
          if (isSSLEnabled()) {
            baseHttpUrl = "https://";
          }
          baseHttpUrl += InetAddress.getLocalHost().getHostName() + ":" + port;
          runtimeInfo.setBaseHttpUrl(baseHttpUrl);
        } catch(UnknownHostException ex) {
          LOG.debug("Exception during hostname resolution: {}", ex);
          runtimeInfo.setBaseHttpUrl(server.getURI().toString());
        }
      }
      for (Connector connector : server.getConnectors()) {
        if (connector instanceof ServerConnector) {
          port = ((ServerConnector)connector).getLocalPort();
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    if (redirector != null) {
      try {
        redirector.start();
        LOG.debug("Running redirector to HTTPS on port '{}'", conf.get(HTTP_PORT_KEY, HTTP_PORT_DEFAULT));
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public URI getServerURI() throws ServerNotYetRunningException {
    if (!server.isStarted()) {
      throw new ServerNotYetRunningException("Server has not yet started");
    } else {
      return server.getURI();
    }
  }

  @Override
  protected void stopTask() {
    try {
      server.stop();
    } catch (Exception ex) {
      LOG.error("Error while stopping Jetty, {}", ex.toString(), ex);
    } finally {
      for (ContextConfigurator cc : contextConfigurators) {
        try {
          cc.stop();
        } catch (Exception ex) {
          LOG.error("Error while stopping '{}', {}", cc.getClass().getSimpleName(), ex.toString(), ex);
        }
      }
    }
    if (redirector != null) {
      try {
        redirector.stop();
      } catch (Exception ex) {
        LOG.error("Error while stopping redirector Jetty, {}", ex.toString(), ex);
      }
    }
  }
}
