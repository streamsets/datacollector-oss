/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.http;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.Configuration;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.authentication.DigestAuthenticator;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.SessionManager;
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
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.Set;

public class WebServerTask extends AbstractTask {
  public static final String HTTP_PORT_KEY = "http.port";
  private static final int HTTP_PORT_DEFAULT = 18630;

  public static final String HTTPS_PORT_KEY = "https.port";
  private static final int HTTPS_PORT_DEFAULT = 0;
  public static final String HTTPS_KEYSTORE_PATH_KEY = "https.keystore.path";
  private static final String HTTPS_KEYSTORE_PATH_DEFAULT = "sdc-keystore.jks";
  public static final String HTTPS_KEYSTORE_PASSWORD_KEY = "https.keystore.password";
  private static final String HTTPS_KEYSTORE_PASSWORD_DEFAULT = "@sdc-keystore-password.txt@";

  public static final String AUTHENTICATION_KEY = "http.authentication";
  public static final String AUTHENTICATION_DEFAULT = "form";

  private static final String DIGEST_REALM_KEY = "http.digest.realm";
  private static final String DIGEST_REALM_DEFAULT = "local-realm";

  private static final String JSESSIONID_COOKIE = "JSESSIONID_";

  private static final Set<String> AUTHENTICATION_MODES = ImmutableSet.of("none", "digest", "form");

  private static final Logger LOG = LoggerFactory.getLogger(WebServerTask.class);

  private final RuntimeInfo runtimeInfo;
  private final Configuration conf;
  private final Set<ContextConfigurator> contextConfigurators;
  private int port;
  private Server server;
  private Server redirector;

  @Inject
  public WebServerTask(RuntimeInfo runtimeInfo, Configuration conf, Set<ContextConfigurator> contextConfigurators) {
    super("webServer");
    this.runtimeInfo = runtimeInfo;
    this.conf = conf;
    this.contextConfigurators = contextConfigurators;
  }

  @Override
  protected void initTask() {
    server = createServer();
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

    SessionManager sm = new HashSessionManager();
    ((HashSessionManager)sm).setSessionCookie(JSESSIONID_COOKIE + port);
    context.setSessionHandler(new SessionHandler(sm));

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
        appHandler.setSecurityHandler(configureDigest(server));
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

  private static final Set<PosixFilePermission> OWNER_PERMISSIONS = ImmutableSet.of(PosixFilePermission.OWNER_EXECUTE,
                                                                                    PosixFilePermission.OWNER_READ,
                                                                                    PosixFilePermission.OWNER_WRITE);

  private void validateRealmFile(File realmFile) {
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
                                              ex.getMessage(), ex));
    }
  }

  private SecurityHandler configureDigest(Server server) {
    String realm = conf.get(DIGEST_REALM_KEY, DIGEST_REALM_DEFAULT);
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
    security.setAuthenticator(new DigestAuthenticator());
    security.setLoginService(loginService);
    return security;
  }

  private SecurityHandler configureForm(Server server) {
    String realm = conf.get(DIGEST_REALM_KEY, DIGEST_REALM_DEFAULT);
    File realmFile = new File(runtimeInfo.getConfigDir(), realm + ".properties").getAbsoluteFile();
    validateRealmFile(realmFile);

    ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();

    Constraint constraint = new Constraint();
    constraint.setName("auth");
    constraint.setAuthenticate(true);
    constraint.setRoles(new String[] { "user"});

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setPathSpec("/*");
    constraintMapping.setConstraint(constraint);

    securityHandler.addConstraintMapping(constraintMapping);

    Constraint noAuthConstraint = new Constraint();
    noAuthConstraint.setName("auth");
    noAuthConstraint.setAuthenticate(false);
    noAuthConstraint.setRoles(new String[] { "user"});


    ConstraintMapping resourceMapping = new ConstraintMapping();
    resourceMapping.setPathSpec("/login");
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

    FormAuthenticator authenticator = new FormAuthenticator("/login.html", "/login.html?error=true", false);
    securityHandler.setAuthenticator(authenticator);
    return securityHandler;
  }

  private boolean isSSLEnabled() {
    return conf.get(HTTPS_PORT_KEY, HTTPS_PORT_DEFAULT) != 0;
  }

  private boolean isRedirectorToSSLEnabled() {
    return conf.get(HTTPS_PORT_KEY, HTTPS_PORT_DEFAULT) != 0 && conf.get(HTTP_PORT_KEY, HTTP_PORT_DEFAULT) != 0;
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
      LOG.debug("Running on port '{}', HTTPS '{}'", port, isSSLEnabled());
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

  @Override
  protected void stopTask() {
    try {
      server.stop();
    } catch (Exception ex) {
      LOG.error("Error while stopping Jetty, {}", ex.getMessage(), ex);
    } finally {
      for (ContextConfigurator cc : contextConfigurators) {
        try {
          cc.stop();
        } catch (Exception ex) {
          LOG.error("Error while stopping '{}', {}", cc.getClass().getSimpleName(), ex.getMessage(), ex);
        }
      }
    }
    if (redirector != null) {
      try {
        redirector.stop();
      } catch (Exception ex) {
        LOG.error("Error while stopping redirector Jetty, {}", ex.getMessage(), ex);
      }
    }
  }
}
