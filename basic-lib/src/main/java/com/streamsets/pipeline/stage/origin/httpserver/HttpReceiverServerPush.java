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
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.http.HttpReceiver;
import com.streamsets.pipeline.lib.http.HttpReceiverServer;
import com.streamsets.pipeline.lib.httpsource.CredentialValueBean;
import com.streamsets.pipeline.lib.httpsource.CredentialValueUserPassBean;
import com.streamsets.pipeline.lib.httpsource.HttpSourceConfigs;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Password;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class HttpReceiverServerPush extends HttpReceiverServer {

  public HttpReceiverServerPush(HttpConfigs configs, HttpReceiver receiver, BlockingQueue<Exception> errorQueue) {
    super(configs, receiver, errorQueue);
  }

  @Override
  public void addReceiverServlet(Stage.Context context, ServletContextHandler contextHandler) {
    super.addReceiverServlet(context, contextHandler);
    SecurityHandler securityHandler = getBasicAuthHandler();
    if(securityHandler!=null)
      contextHandler.setSecurityHandler(securityHandler);

  }

  private SecurityHandler getBasicAuthHandler() {

    if(configs instanceof HttpSourceConfigs && ((HttpSourceConfigs) configs).tlsConfigBean.tlsEnabled){
      HttpSourceConfigs httpCourceConf = (HttpSourceConfigs)configs;
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
      if(empty)
        return null;

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
    return null;
  }
}
