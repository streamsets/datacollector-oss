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
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.DigestAuthenticator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.util.Collections;
import java.util.Set;

public class WebServerTask extends AbstractTask {
  private static final String PORT_NUMBER_KEY = "http.port";
  private static final int PORT_NUMBER_DEFAULT = 8080;

  private static final String AUTHENTICATION_KEY = "http.authentication";
  private static final String AUTHENTICATION_DEFAULT = "none";

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
    port = conf.get(PORT_NUMBER_KEY, PORT_NUMBER_DEFAULT);
    server = new Server(port);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    Handler handler = context;
    context.setContextPath("/");
    server.setHandler(context);
    for (ContextConfigurator cc : contextConfigurators) {
      cc.init(context);
    }
    String auth = conf.get(AUTHENTICATION_KEY, AUTHENTICATION_DEFAULT);
    switch (auth) {
      case "none":
        break;
      case "digest":
        handler = configureDigest(handler);
        break;
      default:
        throw new RuntimeException(Utils.format("Invalid authentication mode '{}', must be one of '{}'",
                                                auth, AUTHENTICATION_MODES));
    }
    server.setHandler(handler);
  }

  private Handler configureDigest(Handler context) {
    String realm = conf.get(DIGEST_REALM_KEY, DIGEST_REALM_DEFAULT);
    File realmFile = new File(runtimeInfo.getConfigDir(), realm + ".properties").getAbsoluteFile();
    if (!realmFile.exists()) {
      throw new RuntimeException(Utils.format("Realm file '{}' does not exists", realmFile));
    }

    LoginService loginService = new HashLoginService(realm, realmFile.getAbsolutePath());
    context.getServer().addBean(loginService);
    ConstraintSecurityHandler security = new ConstraintSecurityHandler();
    context.getServer().setHandler(security);
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
    security.setHandler(context);
    return security;
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
