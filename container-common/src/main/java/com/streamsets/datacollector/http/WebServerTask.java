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
package com.streamsets.datacollector.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.activation.ActivationAuthenticator;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.WebServerAgentCondition;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.DisconnectedSSOManager;
import com.streamsets.lib.security.http.DisconnectedSSOService;
import com.streamsets.lib.security.http.FailoverSSOService;
import com.streamsets.lib.security.http.ProxySSOService;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.SSOAuthenticator;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.lib.security.http.SSOService;
import com.streamsets.lib.security.http.SSOUtils;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.DefaultUserIdentity;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.security.authentication.DigestAuthenticator;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
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
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
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
public abstract class WebServerTask extends AbstractTask {
  public static final String HTTP_BIND_HOST = "http.bindHost";
  private static final String HTTP_BIND_HOST_DEFAULT = "0.0.0.0";

  public static final String HTTP_MAX_THREADS = "http.maxThreads";
  private static final int HTTP_MAX_THREADS_DEFAULT = 200;

  public static final String HTTP_PORT_KEY = "http.port";
  private static final int HTTP_PORT_DEFAULT = 0;

  public static final String HTTPS_PORT_KEY = "https.port";
  private static final int HTTPS_PORT_DEFAULT = -1;
  public static final String HTTPS_KEYSTORE_PATH_KEY = "https.keystore.path";
  private static final String HTTPS_KEYSTORE_PATH_DEFAULT = "keystore.jks";
  public static final String HTTPS_KEYSTORE_PASSWORD_KEY = "https.keystore.password";
  private static final String HTTPS_KEYSTORE_PASSWORD_DEFAULT = "${file(\"keystore-password.txt\")}";
  static final String HTTPS_TRUSTSTORE_PATH_KEY = "https.truststore.path";
  private static final String HTTPS_TRUSTSTORE_PATH_DEFAULT = null;
  private static final String HTTPS_TRUSTSTORE_PASSWORD_KEY = "https.truststore.password";
  private static final String HTTPS_TRUSTSTORE_PASSWORD_DEFAULT = null;

  public static final String HTTP_SESSION_MAX_INACTIVE_INTERVAL_CONFIG = "http.session.max.inactive.interval";
  public static final int HTTP_SESSION_MAX_INACTIVE_INTERVAL_DEFAULT = 86400;  // in seconds = 24 hours
  public static final String HTTP_ENABLE_FORWARDED_REQUESTS_KEY = "http.enable.forwarded.requests";
  private static final boolean HTTP_ENABLE_FORWARDED_REQUESTS_DEFAULT = false;

  public static final String AUTHENTICATION_KEY = "http.authentication";
  public static final String AUTHENTICATION_DEFAULT = "none";

  private static final String DIGEST_REALM_KEY = "http.digest.realm";
  private static final String REALM_POSIX_DEFAULT = "-realm";

  public static final String REALM_FILE_PERMISSION_CHECK = "http.realm.file.permission.check";
  private static final boolean REALM_FILE_PERMISSION_CHECK_DEFAULT = true;

  public static final String HTTP_AUTHENTICATION_LOGIN_MODULE = "http.authentication.login.module";
  public static final String FILE = "file";
  public static final String HTTP_AUTHENTICATION_LOGIN_MODULE_DEFAULT = "file";

  public static final String HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING = "http.authentication.ldap.role.mapping";
  private static final String HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING_DEFAULT = "";

  private static final String JSESSIONID_COOKIE = "JSESSIONID_";

  private static final Set<String> AUTHENTICATION_MODES = ImmutableSet.of("none", "digest", "basic", "form");

  private static final Logger LOG = LoggerFactory.getLogger(WebServerTask.class);
  public static final String LDAP_LOGIN_CONF = "ldap-login.conf";
  public static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
  public static final String LDAP = "ldap";
  public static final String LDAP_LOGIN_MODULE_NAME = "ldap.login.module.name";

  public static final Set<String> LOGIN_MODULES = ImmutableSet.of(FILE, LDAP);

  public static final String SSO_SERVICES_ATTR = "ssoServices";

  private final String serverName;
  private final BuildInfo buildInfo;
  private final RuntimeInfo runtimeInfo;
  private final Configuration conf;
  private final Activation activation;
  private final Set<WebAppProvider> webAppProviders;
  private final Set<ContextConfigurator> contextConfigurators;
  private int port;
  private Server server;
  private HttpConfiguration httpConf = new HttpConfiguration();
  private Server redirector;
  private SessionHandler sessionHandler;
  Map<String, Set<String>> roleMapping;

  public WebServerTask(
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      Configuration conf,
      Activation activation,
      Set<ContextConfigurator> contextConfigurators,
      Set<WebAppProvider> webAppProviders
  ) {
    this("webserver", buildInfo, runtimeInfo, conf, activation, contextConfigurators, webAppProviders);
  }

  public WebServerTask(
      String serverName,
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      Configuration conf,
      Activation activation,
      Set<ContextConfigurator> contextConfigurators,
      Set<WebAppProvider> webAppProviders
  ) {
    super("webServer");
    this.serverName = serverName;
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
    this.conf = conf;
    this.activation = activation;
    this.webAppProviders = webAppProviders;
    this.contextConfigurators = contextConfigurators;
  }

  protected RuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
  }

  protected Configuration getConfiguration() {
    return conf;
  }

  @Override
  public void initTask() {
    checkValidPorts();

    synchronized (getRuntimeInfo()) {
      if (!getRuntimeInfo().hasAttribute(SSO_SERVICES_ATTR)) {
        getRuntimeInfo().setAttribute(SSO_SERVICES_ATTR, Collections.synchronizedList(new ArrayList<>()));
      }
    }

    server = createServer();

    // initialize a global session manager
    sessionHandler = new SessionHandler();
    sessionHandler.setMaxInactiveInterval(conf.get(HTTP_SESSION_MAX_INACTIVE_INTERVAL_CONFIG,
        HTTP_SESSION_MAX_INACTIVE_INTERVAL_DEFAULT));

    ContextHandlerCollection appHandlers = new ContextHandlerCollection();

    // load web apps
    Set<String> contextPaths = new LinkedHashSet<>();
    for (WebAppProvider appProvider : webAppProviders) {
      Configuration appConf = appProvider.getAppConfiguration();
      ServletContextHandler appHandler = appProvider.get();
      String contextPath = appHandler.getContextPath();
      if (contextPath.equals("/")) {
        throw new RuntimeException("Webapps cannot be registered at the root context");
      }
      if (contextPaths.contains(contextPath)) {
        throw new RuntimeException(Utils.format("Webapp already registered at '{}' context", contextPath));
      }
      // all webapps must have a session manager
      appHandler.setSessionHandler(new SessionHandler());

      appHandler.setSecurityHandler(createSecurityHandler(server, appConf, appHandler, contextPath));
      contextPaths.add(contextPath);
      appHandlers.addHandler(appHandler);
    }

    ServletContextHandler appHandler = configureRootContext(sessionHandler);
    appHandler.setSecurityHandler(createSecurityHandler(server, conf, appHandler, "/"));
    Handler handler = configureRedirectionRules(appHandler);
    appHandlers.addHandler(handler);

    server.setHandler(appHandlers);

    if (isRedirectorToSSLEnabled()) {
      redirector = createRedirectorServer();
    }

    addToPostStart(() -> {
      for (WebAppProvider appProvider : webAppProviders) {
        appProvider.postStart();
      }
    });


  }

  @VisibleForTesting
  Server getServer() {
    return server;
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

    // docs is restricted
    ConstraintMapping docMapping = new ConstraintMapping();
    docMapping.setPathSpec("/docs/*");
    docMapping.setConstraint(indexConstraint);

    // Disable TRACE method
    Constraint disableTraceConstraint = new Constraint();
    disableTraceConstraint.setName("Disable TRACE");
    disableTraceConstraint.setAuthenticate(true);
    ConstraintMapping disableTraceMapping = new ConstraintMapping();
    disableTraceMapping.setPathSpec("/*");
    disableTraceMapping.setMethod("TRACE");
    disableTraceMapping.setConstraint(disableTraceConstraint);

    return ImmutableList.of(
        disableTraceMapping,
        restMapping,
        indexMapping,
        docMapping,
        logoutMapping,
        noAuthMapping,
        publicRestMapping
    );
  }

  protected SecurityHandler createSecurityHandler(
      Server server, Configuration appConf, ServletContextHandler appHandler, String appContext
  ) {
    ConstraintSecurityHandler securityHandler;
    String auth = conf.get(AUTHENTICATION_KEY, AUTHENTICATION_DEFAULT);
    boolean isDPMEnabled = runtimeInfo.isDPMEnabled();
    if (isDPMEnabled) {
      securityHandler = configureSSO(appConf, appHandler, appContext);
    } else {
      switch (auth) {
        case "none":
          securityHandler = null;
          break;
        case "digest":
        case "basic":
          securityHandler = configureDigestBasic(appConf, server, auth);
          break;
        case "form":
          securityHandler = configureForm(appConf, server, auth);
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
                                              ex.toString()), ex);
    }
  }

  RemoteSSOService createRemoteSSOService(Configuration appConf) {
    RemoteSSOService remoteSsoService = new RemoteSSOService();
    remoteSsoService.setConfiguration(appConf);
    return remoteSsoService;
  }

  protected boolean isDisconnectedSSOModeEnabled() {
    return false;
  }

  @SuppressWarnings("unchecked")
  private ConstraintSecurityHandler configureSSO(
      final Configuration appConf, ServletContextHandler appHandler, final String appContext
  ) {
    final String componentId = getComponentId(appConf);
    final String appToken = getAppAuthToken(appConf);
    Utils.checkArgument(appToken != null && !appToken.trim().isEmpty(),
        Utils.format("{} cannot be NULL or empty", RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG));

    LOG.debug("Initializing DPM componentId '{}'", componentId);
    ConstraintSecurityHandler security = new ConstraintSecurityHandler();

    final SSOService ssoService;

    RemoteSSOService remoteSsoService = createRemoteSSOService(appConf);
    remoteSsoService.setComponentId(componentId);
    remoteSsoService.setApplicationAuthToken(appToken);
    LOG.info("DPM component ID '{}' application authentication token '{}'", componentId, SSOUtils.tokenForLog
        (appToken));

    if (isDisconnectedSSOModeEnabled()) {
      LOG.info("Support for DPM disconnected mode is enabled");
      DisconnectedSSOManager disconnectedSSOManager =
          new DisconnectedSSOManager(getRuntimeInfo().getDataDir(), appConf);
      disconnectedSSOManager.setEnabled(true);
      disconnectedSSOManager.registerResources(appHandler);
      DisconnectedSSOService disconnectedSSOService = disconnectedSSOManager.getSsoService();

      ssoService = new FailoverSSOService(remoteSsoService, disconnectedSSOService);
    } else {
      LOG.debug("Support for DPM disconnected mode is disabled");
      ssoService = remoteSsoService;
    }

    addToPostStart(() -> {
      LOG.debug("Validating application token for DPM component ID '{}'", componentId);
      ssoService.register(getRegistrationAttributes());
      runtimeInfo.setRemoteRegistrationStatus(true);
    });

    SSOService proxySsoService = new ProxySSOService(ssoService);

    // registering ssoService with runtime, to enable cache flushing
    ((List)getRuntimeInfo().getAttribute(SSO_SERVICES_ATTR)).add(proxySsoService);
    appHandler.getServletContext().setAttribute(SSOService.SSO_SERVICE_KEY, proxySsoService);
    security.setAuthenticator(injectActivationCheck(new SSOAuthenticator(appContext, proxySsoService, appConf)));
    return security;
  }

  protected Authenticator injectActivationCheck(Authenticator authenticator) {
    return (activation == null) ? authenticator : new ActivationAuthenticator(authenticator, activation);
  }

  private ConstraintSecurityHandler configureDigestBasic(Configuration conf, Server server, String mode) {
    LoginService loginService = getLoginService(conf, mode);
    server.addBean(loginService);

    ConstraintSecurityHandler security = new ConstraintSecurityHandler();
    switch (mode) {
      case "digest":
        security.setAuthenticator(injectActivationCheck(new ProxyAuthenticator(
            new DigestAuthenticator(),
            runtimeInfo,
            conf
        )));
        break;
      case "basic":
        security.setAuthenticator(injectActivationCheck(new ProxyAuthenticator(
            new BasicAuthenticator(),
            runtimeInfo,
            conf
        )));
        break;
      default:
        // no action
        break;
    }
    security.setLoginService(loginService);
    return security;
  }

  private ConstraintSecurityHandler configureForm(Configuration conf, Server server, String mode) {
    ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();

    LoginService loginService = getLoginService(conf, mode);
    server.addBean(loginService);
    securityHandler.setLoginService(loginService);

    FormAuthenticator authenticator = new FormAuthenticator("/login.html", "/login.html?error=true", true);
    securityHandler.setAuthenticator(injectActivationCheck(new ProxyAuthenticator(authenticator, runtimeInfo, conf)));
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
    port = isSSLEnabled() ?
      conf.get(HTTPS_PORT_KEY, HTTPS_PORT_DEFAULT) :
      conf.get(HTTP_PORT_KEY, HTTP_PORT_DEFAULT);

    String hostname = conf.get(HTTP_BIND_HOST, HTTP_BIND_HOST_DEFAULT);

    QueuedThreadPool qtp = new QueuedThreadPool(conf.get(HTTP_MAX_THREADS, HTTP_MAX_THREADS_DEFAULT));
    qtp.setName(serverName);
    qtp.setDaemon(true);
    Server server = new Server(qtp);

    httpConf = configureForwardRequestCustomizer(httpConf);

    if (!isSSLEnabled()) {
      InetSocketAddress addr = new InetSocketAddress(hostname, port);
      ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(httpConf));
      connector.setHost(addr.getHostName());
      connector.setPort(addr.getPort());
      server.setConnectors(new Connector[]{connector});
    } else {
      //Create a connector for HTTPS
      httpConf.addCustomizer(new SecureRequestCustomizer());

      SslContextFactory sslContextFactory = createSslContextFactory();
      ServerConnector httpsConnector = new ServerConnector(server,
                                                           new SslConnectionFactory(sslContextFactory, "http/1.1"),
                                                           new HttpConnectionFactory(httpConf));
      httpsConnector.setPort(port);
      httpsConnector.setHost(hostname);
      server.setConnectors(new Connector[]{httpsConnector});
    }
    return server;
  }

  protected SslContextFactory createSslContextFactory() {
    SslContextFactory sslContextFactory = new SslContextFactory();
    File keyStore = getHttpsKeystore(conf, runtimeInfo.getConfigDir());
    if (!keyStore.exists()) {
      throw new RuntimeException(Utils.format("KeyStore file '{}' does not exist", keyStore.getPath()));
    }
    String password = conf.get(HTTPS_KEYSTORE_PASSWORD_KEY, HTTPS_KEYSTORE_PASSWORD_DEFAULT).trim();
    sslContextFactory.setKeyStorePath(keyStore.getPath());
    sslContextFactory.setKeyStorePassword(password);
    sslContextFactory.setKeyManagerPassword(password);
    File trustStoreFile = getHttpsTruststore(conf, runtimeInfo.getConfigDir());
    if (trustStoreFile != null) {
      if (trustStoreFile.exists()) {
        sslContextFactory.setTrustStorePath(trustStoreFile.getPath());
        String trustStorePassword = Utils.checkNotNull(conf.get(HTTPS_TRUSTSTORE_PASSWORD_KEY,
            HTTPS_TRUSTSTORE_PASSWORD_DEFAULT
        ), HTTPS_TRUSTSTORE_PASSWORD_KEY);
        sslContextFactory.setTrustStorePassword(trustStorePassword.trim());
      } else {
        throw new IllegalStateException(Utils.format(
            "Truststore file: '{}' " + "doesn't exist",
            trustStoreFile.getAbsolutePath()
        ));
      }
    }
    return sslContextFactory;
  }

  private void setSSLContext() {
    for (Connector connector : server.getConnectors()) {
      for (ConnectionFactory connectionFactory : connector.getConnectionFactories()) {
        if (connectionFactory instanceof SslConnectionFactory) {
          runtimeInfo.setSSLContext(((SslConnectionFactory) connectionFactory).getSslContextFactory().getSslContext());
        }
      }
    }
    if (runtimeInfo.getSSLContext() == null) {
      throw new IllegalStateException("Unexpected error, SSLContext is not set for https enabled server");
    }
  }

  private File getHttpsTruststore(Configuration conf, String configDir) {
    final String httpsTruststorePath = conf.get(HTTPS_TRUSTSTORE_PATH_KEY, HTTPS_TRUSTSTORE_PATH_DEFAULT);
    if (httpsTruststorePath == null || httpsTruststorePath.trim().isEmpty()) {
      LOG.info(Utils.format(
          "TrustStore config '{}' is not set, will pickup" + " truststore from $JAVA_HOME/jre/lib/security/cacerts",
          HTTPS_TRUSTSTORE_PATH_KEY
      ));
      return null;
    } else if (Paths.get(httpsTruststorePath).isAbsolute()) {
      return new File(httpsTruststorePath).getAbsoluteFile();
    } else {
      return new File(configDir, httpsTruststorePath).getAbsoluteFile();
    }
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
    String hostname = conf.get(HTTP_BIND_HOST, HTTP_BIND_HOST_DEFAULT);

    QueuedThreadPool qtp = new QueuedThreadPool(25);
    qtp.setName(serverName + "Redirector");
    qtp.setDaemon(true);
    Server server = new Server(qtp);
    InetSocketAddress addr = new InetSocketAddress(hostname, unsecurePort);
    ServerConnector connector = new ServerConnector(server);
    connector.setHost(addr.getHostName());
    connector.setPort(addr.getPort());
    server.setConnectors(new Connector[]{connector});

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

  protected String getHttpUrl() {
    return runtimeInfo.getBaseHttpUrl();
  }

  @Override
  protected void runTask() {
    runTaskInternal();
    try {
      WebServerAgentCondition.waitForCredentials();
    } catch (InterruptedException ex) {
      LOG.error("Interrupted while waiting for credentials to be deployed", ex);
    }
  }

  private void runTaskInternal() {
    for (ContextConfigurator cc : contextConfigurators) {
      cc.start();
    }
    try {
      server.start();
      port = server.getURI().getPort();
      sessionHandler.setSessionCookie(JSESSIONID_COOKIE + port);
      if(runtimeInfo.getBaseHttpUrl().equals(RuntimeInfo.UNDEF)) {
        try {
          String baseHttpUrl = "http://";
          if (isSSLEnabled()) {
            baseHttpUrl = "https://";
          }
          String hostname = conf.get(HTTP_BIND_HOST, HTTP_BIND_HOST_DEFAULT);
          baseHttpUrl += !"0.0.0.0".equals(hostname) ? hostname : InetAddress.getLocalHost().getCanonicalHostName();
          baseHttpUrl += ":" + port;
          runtimeInfo.setBaseHttpUrl(baseHttpUrl);
        } catch(UnknownHostException ex) {
          LOG.debug("Exception during hostname resolution: {}", ex);
          runtimeInfo.setBaseHttpUrl(server.getURI().toString());
        }
      }
      System.out.println(Utils.format("Running on URI : '{}'", getHttpUrl()));
      LOG.info("Running on URI : '{}'", getHttpUrl());
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
    if (isSSLEnabled()) {
      setSSLContext();
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

  protected LoginService getLoginService(Configuration conf, String mode) {
    LoginService loginService = null;
    String loginModule = this.conf.get(HTTP_AUTHENTICATION_LOGIN_MODULE, HTTP_AUTHENTICATION_LOGIN_MODULE_DEFAULT);
    switch (loginModule) {
      case FILE:
        String realm = conf.get(DIGEST_REALM_KEY, mode + REALM_POSIX_DEFAULT);
        File realmFile = new File(runtimeInfo.getConfigDir(), realm + ".properties").getAbsoluteFile();
        validateRealmFile(realmFile);
        loginService = new SdcHashLoginService(realm, realmFile.getAbsolutePath());
        break;
      case LDAP:
        // If “java.security.auth.login.config” system property is set then use that config file.
        // This will allow users to include sdc ldap entry in their existing ldap login config file.
        // If not, pick up login config file from location ${SDC_DIST}/etc/ldap-login.conf
        String ldapConfigFileName = System.getProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, null);
        if (null == ldapConfigFileName) {
          File ldapConfigFile = new File(runtimeInfo.getConfigDir(), LDAP_LOGIN_CONF).getAbsoluteFile();
          System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, ldapConfigFile.getAbsolutePath());
        }

        roleMapping = parseRoleMapping(conf.get(HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING,
            HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING_DEFAULT));

        // Look up the login module name from sdc.properties file. Assume "ldap" if none specified.
        // This helps to be backward compatible and allows for overriding the login module name if the jaas config
        // file contains multiple entries
        String loginModuleName = conf.get(LDAP_LOGIN_MODULE_NAME, LDAP);
        if (loginModuleName.trim().isEmpty()) {
          loginModuleName = LDAP;
        }

        // resetting it becuase it is cached and testcases fail then
        javax.security.auth.login.Configuration.getConfiguration().setConfiguration(null);
        // verifying that is authentication mode is DIGEST and we are using LDAP, we don't allow forceBindingLogin
        // set to TRUE as it won't work
        if ("digest".equals(mode)) {
          AppConfigurationEntry configEntries[] =
              javax.security.auth.login.Configuration.getConfiguration().getAppConfigurationEntry(loginModuleName);
          if (configEntries.length == 1) {
            String forceBindingLogin = (String) configEntries[0].getOptions().get("forceBindingLogin");
            if (forceBindingLogin != null && Boolean.parseBoolean(forceBindingLogin.trim())) {
              throw new RuntimeException(
                  "Digest authentication cannot be used with LDAP 'forceBindingLoging' set to true");
            }
          }
        }


        loginService = new JAASLoginService(loginModuleName);

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
        roleMapping.computeIfAbsent(ldapRole, k -> new HashSet<>());
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
    Set<String> roles = new HashSet<>();
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

  protected abstract String getAppAuthToken(Configuration appConfiguration);

  protected abstract String getComponentId(Configuration appConfiguration);

  protected Map<String, String> getRegistrationAttributes() {
    return ImmutableMap.of(SSOConstants.SERVICE_BASE_URL_ATTR, this.runtimeInfo.getBaseHttpUrl());
  }

  @VisibleForTesting
  HttpConfiguration configureForwardRequestCustomizer(HttpConfiguration httpConf) {
    if (conf.get(HTTP_ENABLE_FORWARDED_REQUESTS_KEY, HTTP_ENABLE_FORWARDED_REQUESTS_DEFAULT)) {
      httpConf.addCustomizer(new ForwardedRequestCustomizer());
    }

    return httpConf;
  }

  @VisibleForTesting
  HttpConfiguration getHttpConf() {
    return httpConf;
  }
}
