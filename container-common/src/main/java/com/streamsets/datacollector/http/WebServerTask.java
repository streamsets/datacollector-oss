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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.ProxySSOService;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.SSOAuthenticator;
import com.streamsets.lib.security.http.SSOService;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.DefaultUserIdentity;
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
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.session.HashSessionManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.security.auth.Subject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Automatic security configuration based on URL paths:
 *
 * public    /*
 * public    /public-rest/*
 * protected /rest/*
 * public    /<APP>/*
 * public    /<APP>/public-rest/*
 * protected /<APP>/rest/*
 *
 * public means authentication IS NOT required.
 * protected means authentication IS required.
 *
 */
public class WebServerTask extends AbstractTask {
  public static final String HTTP_PORT_KEY = "http.port";
  private static final int HTTP_PORT_DEFAULT = 0;

  public static final String HTTPS_PORT_KEY = "https.port";
  private static final int HTTPS_PORT_DEFAULT = -1;
  public static final String HTTPS_KEYSTORE_PATH_KEY = "https.keystore.path";
  private static final String HTTPS_KEYSTORE_PATH_DEFAULT = "keystore.jks";
  public static final String HTTPS_KEYSTORE_PASSWORD_KEY = "https.keystore.password";
  private static final String HTTPS_KEYSTORE_PASSWORD_DEFAULT = "@keystore-password.txt@";

  public static final String HTTP_SESSION_MAX_INACTIVE_INTERVAL_CONFIG = "http.session.max.inactive.interval";
  public static final int HTTP_SESSION_MAX_INACTIVE_INTERVAL_DEFAULT = 86400;  // in seconds = 24 hours

  public static final String AUTHENTICATION_KEY = "http.authentication";
  public static final String AUTHENTICATION_DEFAULT = "none"; //"form";

  private static final String DIGEST_REALM_KEY = "http.digest.realm";
  private static final String REALM_POSIX_DEFAULT = "-realm";

  public static final String REALM_FILE_PERMISSION_CHECK = "http.realm.file.permission.check";
  private static final boolean REALM_FILE_PERMISSION_CHECK_DEFAULT = true;

  public static final String HTTP_AUTHENTICATION_LOGIN_MODULE = "http.authentication.login.module";
  private static final String HTTP_AUTHENTICATION_LOGIN_MODULE_DEFAULT = "file";

  public static final String HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING = "http.authentication.ldap.role.mapping";
  private static final String HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING_DEFAULT = "";


  public static final String REMOTE_APPLICATION_TOKEN = "dpm.applicationToken";
  public static final String REMOTE_APPLICATION_TOKEN_DEFAULT = "";

  public static final String DPM_ENABLED = "dpm.enabled";
  public static final boolean DPM_ENABLED_DEFAULT = false;

  private static final String JSESSIONID_COOKIE = "JSESSIONID_";

  private static final Set<String> AUTHENTICATION_MODES = ImmutableSet.of("none", "digest", "basic", "form");

  private static final Set<String> LOGIN_MODULES = ImmutableSet.of("file", "ldap");

  private static final Logger LOG = LoggerFactory.getLogger(WebServerTask.class);

  private final RuntimeInfo runtimeInfo;
  private final Configuration conf;
  private final Set<WebAppProvider> webAppProviders;
  private final Set<ContextConfigurator> contextConfigurators;
  private int port;
  private Server server;
  private Server redirector;
  private HashSessionManager hashSessionManager;
  private Map<String, Set<String>> roleMapping;

  @Inject
  public WebServerTask(
      RuntimeInfo runtimeInfo,
      Configuration conf,
      Set<ContextConfigurator> contextConfigurators,
      Set<WebAppProvider> webAppProviders
  ) {
    super("webServer");
    this.runtimeInfo = runtimeInfo;
    this.conf = conf;
    this.webAppProviders = webAppProviders;
    this.contextConfigurators = contextConfigurators;
  }

  @Override
  public void initTask() {
    checkValidPorts();
    server = createServer();

    // initialize a global session manager
    hashSessionManager = new HashSessionManager();
    hashSessionManager.setMaxInactiveInterval(conf.get(HTTP_SESSION_MAX_INACTIVE_INTERVAL_CONFIG,
        HTTP_SESSION_MAX_INACTIVE_INTERVAL_DEFAULT));

    ContextHandlerCollection appHandlers = new ContextHandlerCollection();

    // load web apps
    Set<String> contextPaths = new LinkedHashSet<>();
    for (WebAppProvider appProvider : webAppProviders) {
      ServletContextHandler appHandler = appProvider.get();
      String contextPath = appHandler.getContextPath();
      if (contextPath.equals("/")) {
        throw new RuntimeException("Webapps cannot be registered at the root context");
      }
      if (contextPaths.contains(contextPath)) {
        throw new RuntimeException(Utils.format("Webapp already registered at '{}' context", contextPath));
      }
      // all webapps must have a session manager
      appHandler.setSessionHandler(new SessionHandler(hashSessionManager));

      appHandler.setSecurityHandler(createSecurityHandler(server, appHandler, contextPath));
      contextPaths.add(contextPath);
      appHandlers.addHandler(appHandler);
    }

    ServletContextHandler appHandler = configureRootContext(new SessionHandler(hashSessionManager));
    appHandler.setSecurityHandler(createSecurityHandler(server, appHandler, "/"));
    Handler handler = configureRedirectionRules(appHandler);
    appHandlers.addHandler(handler);

    server.setHandler(appHandlers);

    if (isRedirectorToSSLEnabled()) {
      redirector = createRedirectorServer();
    }

    addToPostStart(new Runnable() {
      @Override
      public void run() {
        for (WebAppProvider appProvider : webAppProviders) {
          appProvider.postStart();
        }
      }
    });


  }

  private ServletContextHandler configureRootContext(SessionHandler sessionHandler) {
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);

    context.setSessionHandler(sessionHandler);

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

  private List<ConstraintMapping> createConstraintMappings() {
    // everything under /* public
    Constraint noAuthConstraint = new Constraint();
    noAuthConstraint.setName("auth");
    noAuthConstraint.setAuthenticate(false);
    noAuthConstraint.setRoles(new String[]{"user"});
    ConstraintMapping noAuthMapping = new ConstraintMapping();
    noAuthMapping.setPathSpec("/*");
    noAuthMapping.setConstraint(noAuthConstraint);

    // everything under /public-rest/* public
    Constraint publicRestConstraint = new Constraint();
    publicRestConstraint.setName("auth");
    publicRestConstraint.setAuthenticate(false);
    publicRestConstraint.setRoles(new String[] { "user"});
    ConstraintMapping publicRestMapping = new ConstraintMapping();
    publicRestMapping.setPathSpec("/public-rest/*");
    publicRestMapping.setConstraint(publicRestConstraint);


    // everything under /rest/* restricted
    Constraint restConstraint = new Constraint();
    restConstraint.setName("auth");
    restConstraint.setAuthenticate(true);
    restConstraint.setRoles(new String[] { "user"});
    ConstraintMapping restMapping = new ConstraintMapping();
    restMapping.setPathSpec("/rest/*");
    restMapping.setConstraint(restConstraint);

    // /logout is restricted
    Constraint logoutConstraint = new Constraint();
    logoutConstraint.setName("auth");
    logoutConstraint.setAuthenticate(true);
    logoutConstraint.setRoles(new String[] { "user"});
    ConstraintMapping logoutMapping = new ConstraintMapping();
    logoutMapping.setPathSpec("/logout");
    logoutMapping.setConstraint(logoutConstraint);

    // index page is restricted to trigger login correctly when using form authentication
    Constraint indexConstraint = new Constraint();
    indexConstraint.setName("auth");
    indexConstraint.setAuthenticate(true);
    indexConstraint.setRoles(new String[] { "user"});
    ConstraintMapping indexMapping = new ConstraintMapping();
    indexMapping.setPathSpec("");
    indexMapping.setConstraint(indexConstraint);

    return ImmutableList.of(restMapping, indexMapping, logoutMapping, noAuthMapping, publicRestMapping);
  }


  private SecurityHandler createSecurityHandler(Server server, ServletContextHandler appHandler, String appContext) {
    ConstraintSecurityHandler securityHandler;
    String auth = conf.get(AUTHENTICATION_KEY, AUTHENTICATION_DEFAULT);
    boolean isDPMEnabled = conf.get(DPM_ENABLED, DPM_ENABLED_DEFAULT);
    if (isDPMEnabled) {
      securityHandler = configureSSO(appHandler, appContext);
    } else {
      switch (auth) {
        case "none":
          securityHandler = null;
          break;
        case "digest":
        case "basic":
          securityHandler = configureDigestBasic(server, auth);
          break;
        case "form":
          securityHandler = configureForm(server, auth);
          break;
        case "sso":
          securityHandler = configureSSO(appHandler, appContext);
          break;
        default:
          throw new RuntimeException(Utils.format("Invalid authentication mode '{}', must be one of '{}'",
              auth, AUTHENTICATION_MODES));
      }
    }
    if (securityHandler != null) {
      List<ConstraintMapping> constraintMappings = new ArrayList<>();
      constraintMappings.addAll(createConstraintMappings());
      securityHandler.setConstraintMappings(constraintMappings);
    }
    return securityHandler;
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

  private ConstraintSecurityHandler configureSSO(ServletContextHandler appHandler, String appContext) {
    addToPostStart(new Runnable() {
      @Override
      public void run() {
        LOG.debug("Validating Application Token with Remote Service");
        validateApplicationToken();
      }
    });

    ConstraintSecurityHandler security = new ConstraintSecurityHandler();
    final SSOService ssoService = new ProxySSOService(new RemoteSSOService());
    appHandler.getServletContext().setAttribute(SSOService.SSO_SERVICE_KEY, ssoService);
    addToPostStart(new Runnable() {
      @Override
      public void run() {
        LOG.debug("Initializing SSO service");
        ssoService.setConfiguration(conf);
      }
    });
    security.setAuthenticator(new SSOAuthenticator(appContext, ssoService, conf));
    return security;
  }

  private ConstraintSecurityHandler configureDigestBasic(Server server, String mode) {
    LoginService loginService = getLoginService(mode);
    server.addBean(loginService);

    ConstraintSecurityHandler security = new ConstraintSecurityHandler();
    switch (mode) {
      case "digest":
        security.setAuthenticator(new ProxyAuthenticator(new DigestAuthenticator(), runtimeInfo, conf));
        break;
      case "basic":
        security.setAuthenticator(new ProxyAuthenticator(new BasicAuthenticator(), runtimeInfo, conf));
        break;
      default:
        // no action
        break;
    }
    security.setLoginService(loginService);
    return security;
  }

  private ConstraintSecurityHandler configureForm(Server server, String mode) {
    ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();

    LoginService loginService = getLoginService(mode);
    server.addBean(loginService);
    securityHandler.setLoginService(loginService);

    FormAuthenticator authenticator = new FormAuthenticator("/login.html", "/login.html?error=true", true);
    securityHandler.setAuthenticator(new ProxyAuthenticator(authenticator, runtimeInfo, conf));
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

      File keyStore = getHttpsKeystore(conf, runtimeInfo.getConfigDir());

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

  @VisibleForTesting
  static File getHttpsKeystore(Configuration conf, String configDir) {
    final String httpsKeystorePath = conf.get(HTTPS_KEYSTORE_PATH_KEY, HTTPS_KEYSTORE_PATH_DEFAULT);
    if (Paths.get(httpsKeystorePath).isAbsolute()) {
      return new File(httpsKeystorePath).getAbsoluteFile();
    } else {
      return new File(configDir, httpsKeystorePath).getAbsoluteFile();
    }
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

  private final List<Runnable> postStartRunnables = new ArrayList<>();

  void addToPostStart(Runnable runnable) {
    postStartRunnables.add(runnable);
  }

  void postStart() {
    for (Runnable runnable : postStartRunnables) {
      runnable.run();
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
      if(runtimeInfo.getBaseHttpUrl().equals(RuntimeInfo.UNDEF)) {
        try {
          String baseHttpUrl = "http://";
          if (isSSLEnabled()) {
            baseHttpUrl = "https://";
          }
          baseHttpUrl += InetAddress.getLocalHost().getCanonicalHostName() + ":" + port;
          runtimeInfo.setBaseHttpUrl(baseHttpUrl);
          LOG.info("Running on URI : '{}'", baseHttpUrl);
          System.out.println(Utils.format("Running on URI : '{}'", baseHttpUrl));
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
        LOG.debug("Running HTTP redirector to HTTPS on port '{}'", conf.get(HTTP_PORT_KEY, HTTP_PORT_DEFAULT));
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    postStart();
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

  private LoginService getLoginService(String mode) {
    LoginService loginService = null;
    String loginModule = this.conf.get(HTTP_AUTHENTICATION_LOGIN_MODULE, HTTP_AUTHENTICATION_LOGIN_MODULE_DEFAULT);
    switch (loginModule) {
      case "file":
        String realm = conf.get(DIGEST_REALM_KEY, mode + REALM_POSIX_DEFAULT);
        File realmFile = new File(runtimeInfo.getConfigDir(), realm + ".properties").getAbsoluteFile();
        validateRealmFile(realmFile);
        loginService = new HashLoginService(realm, realmFile.getAbsolutePath());
        break;
      case "ldap":
        File ldapConfigFile = new File(runtimeInfo.getConfigDir(), "ldap-login.conf").getAbsoluteFile();
        System.setProperty("java.security.auth.login.config", ldapConfigFile.getAbsolutePath());

        roleMapping = parseRoleMapping(conf.get(HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING,
            HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING_DEFAULT));

        loginService = new JAASLoginService("ldap");
        loginService.setIdentityService(new DefaultIdentityService() {
          @Override
          public UserIdentity newUserIdentity(Subject subject, Principal userPrincipal, String[] roles) {
            Set<String> rolesSet = new HashSet<>();
            rolesSet.add("user");
            for(String role: roles) {
              Set<String> dcRoles = tryMappingRole(role);
              if(dcRoles != null && dcRoles.size() > 0) {
                rolesSet.addAll(dcRoles);
              } else {
                rolesSet.add(role);
              }
            }
            return new DefaultUserIdentity(subject, userPrincipal, rolesSet.toArray(new String[rolesSet.size()]));
          }
        });
        break;
      default:
        throw new RuntimeException(Utils.format("Invalid Authentication Login Module '{}', must be one of '{}'",
            loginModule, LOGIN_MODULES));
    }
    return loginService;
  }

  private Map<String, Set<String>> parseRoleMapping(String option) {
    if(option == null || option.trim().length() == 0) {
      throw new RuntimeException(Utils.format("LDAP group to Data Collector role mapping configuration - '{}' is empty",
          HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING));
    }
    Map<String, Set<String>> roleMapping = new HashMap<>();
    try {
      String[] mappings = option.split(";");
      for (String mapping : mappings) {
        String[] map = mapping.split(":", 2);
        String ldapRole = map[0].trim();
        String[] streamSetsRoles = map[1].split(",");
        if (roleMapping.get(ldapRole) == null) {
          roleMapping.put(ldapRole, new HashSet<String>());
        }
        final Set<String> streamSetsRolesSet = roleMapping.get(ldapRole);
        for (String streamSetsRole : streamSetsRoles) {
          streamSetsRolesSet.add(streamSetsRole.trim());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(Utils.format("Invalid LDAP group to Data Collector role mapping configuration - '{}'.",
           option, e.getMessage()), e);
    }
    return roleMapping;
  }

  protected Set<String> tryMappingRole(String role) {
    Set<String> roles = new HashSet<String>();
    if (roleMapping == null || roleMapping.isEmpty()) {
      return roles;
    }
    Set<String> streamSetsRoles = roleMapping.get(role);
    if (streamSetsRoles != null) {
      // add all mapped roles
      for (String streamSetsRole : streamSetsRoles) {
        roles.add(streamSetsRole);
      }
    }
    return roles;
  }

  private void validateApplicationToken() {
    String applicationToken = conf.get(REMOTE_APPLICATION_TOKEN, REMOTE_APPLICATION_TOKEN_DEFAULT);

    if (applicationToken != null && applicationToken.trim().length() > 0 &&
        conf.hasName(RemoteSSOService.DPM_BASE_URL_CONFIG)) {
      String dpmBaseURL = RemoteSSOService.getValidURL(conf.get(RemoteSSOService.DPM_BASE_URL_CONFIG,
          RemoteSSOService.DPM_BASE_URL_DEFAULT));
      String registrationURI = dpmBaseURL + "security/public-rest/v1/components/registration";

      Map<String, Object> registrationData = new HashMap<>();
      registrationData.put("authToken", applicationToken);
      registrationData.put("componentId", this.runtimeInfo.getId());
      registrationData.put("attributes", ImmutableMap.of("baseHttpUrl", this.runtimeInfo.getBaseHttpUrl()));
      Response response = ClientBuilder.newClient()
          .target(registrationURI)
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .post(Entity.json(registrationData));

      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException("Registration to Remote Service failed : " +
            response.readEntity(String.class));
      }

      this.runtimeInfo.setRemoteRegistrationStatus(true);
    }
  }
}
