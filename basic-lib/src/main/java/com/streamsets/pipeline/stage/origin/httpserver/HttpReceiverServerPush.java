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
package com.streamsets.pipeline.stage.origin.httpserver;


import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.http.HttpReceiver;
import com.streamsets.pipeline.lib.http.HttpReceiverServer;
import com.streamsets.pipeline.lib.httpsource.CredentialValueUserPassBean;
import com.streamsets.pipeline.lib.httpsource.HttpSourceConfigs;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.SpnegoLoginService;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.security.authentication.SpnegoAuthenticator;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Password;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class HttpReceiverServerPush extends HttpReceiverServer {

  private static final String TARGET_NAME_FIELD_NAME = "_targetName";
  private static final String JAVAX_SECURITY_AUTH_USE_SUBJECT_CREDS_ONLY = "javax.security.auth.useSubjectCredsOnly";
  private static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
  private static final String JGSS_ACCEPT = "com.sun.security.jgss.accept {\n" +
      "     com.sun.security.auth.module.Krb5LoginModule required\n" +
      "     principal=\"%s\"\n" +
      "     useKeyTab=true\n" +
      "     keyTab=\"%s\"\n" +
      "     storeKey=true\n" +
      "     debug=true\n" +
      "     isInitiator=false;\n" +
      "};";
  private static final String JGSS_INITITATE = "com.sun.security.jgss.initiate {\n" +
      "     com.sun.security.auth.module.Krb5LoginModule required\n" +
      "     principal=\"%s\"\n" +
      "     keyTab=\"%s\"\n" +
      "     useKeyTab=true\n" +
      "     storeKey=true\n" +
      "     debug=true\n" +
      "     isInitiator=false;\n" +
      "};";

  public HttpReceiverServerPush(HttpConfigs configs, HttpReceiver receiver, BlockingQueue<Exception> errorQueue) {
    super(configs, receiver, errorQueue);
  }

  @Override
  public void addReceiverServlet(Stage.Context context, ServletContextHandler contextHandler) {
    super.addReceiverServlet(context, contextHandler);
    HttpSourceConfigs httpSourceConfigs = (HttpSourceConfigs) configs;
    SecurityHandler securityHandler =
        httpSourceConfigs.spnegoConfigBean.isSpnegoEnabled() ? getSpnegoAuthHandler(httpSourceConfigs, context) :
            httpSourceConfigs.tlsConfigBean.isEnabled() ? getBasicAuthHandler(httpSourceConfigs) : null;
    if(securityHandler!=null) {
      contextHandler.setSecurityHandler(securityHandler);
    }
  }

  public static SecurityHandler getSpnegoAuthHandler(HttpSourceConfigs httpCourceConf, Stage.Context context) throws StageException {
    String domainRealm = httpCourceConf.getSpnegoConfigBean().getKerberosRealm();
    String principal = httpCourceConf.getSpnegoConfigBean().getSpnegoPrincipal();
    String keytab = httpCourceConf.getSpnegoConfigBean().getSpnegoKeytabFilePath();

    File f = new File(context.getResourcesDirectory()+"/spnego.conf");
    try {
      PrintWriter pw = new PrintWriter(f);
      pw.println(String.format(JGSS_INITITATE ,principal,keytab) +"\n"+ String.format(JGSS_ACCEPT,principal,keytab));
      pw.close();
    } catch (IOException e) {
      throw new StageException(Errors.HTTP_36, e);
    }

    System.setProperty(JAVAX_SECURITY_AUTH_USE_SUBJECT_CREDS_ONLY, "false");
    System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, context.getResourcesDirectory()+"/spnego.conf");

    Constraint constraint = new Constraint();
    constraint.setName(Constraint.__SPNEGO_AUTH);
    constraint.setRoles(new String[]{domainRealm});
    constraint.setAuthenticate(true);

    ConstraintMapping cm = new ConstraintMapping();
    cm.setConstraint(constraint);
    cm.setPathSpec("/*");

    SpnegoLoginService loginService = new SpnegoLoginService(){
      @Override
      protected void doStart() throws Exception {
        // Override the parent implementation to set the targetName without having
        // an extra .properties file.
        final Field targetNameField = SpnegoLoginService.class.getDeclaredField(TARGET_NAME_FIELD_NAME);
        targetNameField.setAccessible(true);
        targetNameField.set(this, principal);
      }
    };
    loginService.setName(domainRealm);

    ConstraintSecurityHandler csh = new ConstraintSecurityHandler();
    csh.setAuthenticator(new SpnegoAuthenticator());
    csh.setLoginService(loginService);
    csh.setConstraintMappings(new ConstraintMapping[]{cm});
    csh.setRealmName(domainRealm);

    return csh;
  }

  public static SecurityHandler getBasicAuthHandler(HttpSourceConfigs httpCourceConf) {
    List<CredentialValueUserPassBean> basicAuthUsers = httpCourceConf.getBasicAuthUsers();

    HashLoginService loginService = new HashLoginService();
    UserStore userStore = new UserStore();

    boolean empty = true;
    for (CredentialValueUserPassBean userPassBean : basicAuthUsers) {
      String username = userPassBean.getUsername();
      String password = userPassBean.get();
      if(StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
        userStore.addUser(username, new Password(password), new String[]{"sdc"});
        empty = false;
      }
    }
    if(empty) {
      return null;
    }

    loginService.setUserStore(userStore);

    Constraint constraint = new Constraint(Constraint.__BASIC_AUTH,"sdc");
    constraint.setAuthenticate(true);

    ConstraintMapping mapping = new ConstraintMapping();
    mapping.setConstraint(constraint);
    mapping.setPathSpec("/*");

    ConstraintSecurityHandler handler = new ConstraintSecurityHandler();
    handler.setAuthenticator(new BasicAuthenticator());
    handler.addConstraintMapping(mapping);
    handler.setLoginService(loginService);

    return handler;
  }
}