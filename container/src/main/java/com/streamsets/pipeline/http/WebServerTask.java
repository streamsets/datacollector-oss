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
import org.eclipse.jetty.security.authentication.DigestAuthenticator;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.Set;

public class WebServerTask extends AbstractTask {
  private static final String PORT_NUMBER_KEY = "http.port";
  private static final int PORT_NUMBER_DEFAULT = 8080;

  private static final String AUTHENTICATION_KEY = "http.authentication";
  private static final String AUTHENTICATION_DEFAULT = "form";

  private static final String DIGEST_REALM_KEY = "http.digest.realm";
  private static final String DIGEST_REALM_DEFAULT = "local-realm";

  private static final Set<String> AUTHENTICATION_MODES = ImmutableSet.of("none", "digest");

  private static final Logger LOG = LoggerFactory.getLogger(WebServerTask.class);

  private final RuntimeInfo runtimeInfo;
  private final Configuration conf;
  private final Set<ContextConfigurator> contextConfigurators;
  private int port;
  private Server server;

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
    Handler handler = configureAppContext();
    handler = configureAuthentication(server, (ServletContextHandler)handler);
    handler = configureRedirectionRules(handler);
    server.setHandler(handler);
  }

  private Handler configureAppContext() {
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
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
    Handler handler;
    String auth = conf.get(AUTHENTICATION_KEY, AUTHENTICATION_DEFAULT);
    switch (auth) {
      case "none":
        handler = appHandler;
        break;
      case "digest":
        handler = configureDigest(server, appHandler);
        break;
      case "form":
        handler = configureForm(server, appHandler);
        break;
      default:
        throw new RuntimeException(Utils.format("Invalid authentication mode '{}', must be one of '{}'",
                                                auth, AUTHENTICATION_MODES));
    }
    return handler;
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

  private Handler configureDigest(Server server, ServletContextHandler appHandler) {
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
    appHandler.setSecurityHandler(security);
    return appHandler;
  }

  private Handler configureForm(Server server, ServletContextHandler appHandler) {
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

    appHandler.setSecurityHandler(securityHandler);
    return appHandler;
  }

  private Server createServer() {
    //TODO support HTTPS configuration
    port = conf.get(PORT_NUMBER_KEY, PORT_NUMBER_DEFAULT);
    return new Server(port);
  }

  @Override
  protected void runTask() {
    for (ContextConfigurator cc : contextConfigurators) {
      cc.start();
    }
    try {
      server.start();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    LOG.debug("Running on port '{}'", port);
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
  }
}
