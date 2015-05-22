/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.http;

import com.streamsets.pipeline.main.RuntimeInfo;
import org.eclipse.jetty.security.*;
import org.eclipse.jetty.security.authentication.LoginAuthenticator;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.UserIdentity;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.security.Principal;


public class ProxyAuthenticator extends LoginAuthenticator {
  private final RuntimeInfo runtimeInfo;
  private final LoginAuthenticator authenticator;
  public final static String AUTH_TOKEN = "auth_token";
  public final static String AUTH_USER = "auth_user";
  private final static String TOKEN_AUTHENTICATION_USER_NAME = "admin";

  public ProxyAuthenticator(LoginAuthenticator authenticator, RuntimeInfo runtimeInfo) {
    this.authenticator = authenticator;
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public void prepareRequest(ServletRequest request) {
    super.prepareRequest(request);
    authenticator.prepareRequest(request);
  }

  @Override
  public UserIdentity login(String username, Object password, ServletRequest request) {
    return authenticator.login(username, password, request);
  }

  @Override
  public void setConfiguration(AuthConfiguration configuration) {
    super.setConfiguration(configuration);
    authenticator.setConfiguration(configuration);
  }

  @Override
  public LoginService getLoginService() {
    return authenticator.getLoginService();
  }

  @Override
  protected HttpSession renewSession(HttpServletRequest request, HttpServletResponse response) {
    return super.renewSession(request, response);
  }


  @Override
  public String getAuthMethod() {
    return authenticator.getAuthMethod();
  }

  @Override
  public boolean secureResponse(ServletRequest request, ServletResponse response, boolean mandatory, Authentication.User validatedUser) throws ServerAuthException {
    return authenticator.secureResponse(request, response, mandatory, validatedUser);
  }

  @Override
  public Authentication validateRequest(ServletRequest req, ServletResponse res, boolean mandatory)
    throws ServerAuthException {

    HttpServletRequest request = (HttpServletRequest)req;
    HttpSession session = request.getSession(true);
    Authentication authentication = (Authentication) session.getAttribute(SessionAuthentication.__J_AUTHENTICATED);

    if (authentication == null) {
      String authToken = request.getHeader(AUTH_TOKEN);
      String authUser = request.getHeader(AUTH_USER);

      if(authToken == null) {
        authToken = request.getParameter(AUTH_TOKEN);
        authUser = request.getParameter(AUTH_USER);

        if(authUser == null) {
          authUser = TOKEN_AUTHENTICATION_USER_NAME;
        }
      }

      if(authToken != null && runtimeInfo.isValidAuthenticationToken(authToken)) {
        //TODO: Get user name from Request header and use that name for creating Principal
        Principal userPrincipal = new MappedLoginService.KnownUser(authUser, null);
        Subject subject = new Subject();
        subject.getPrincipals().add(userPrincipal);
        subject.setReadOnly();
        String [] roles = runtimeInfo.getRolesFromAuthenticationToken(authToken);
        UserIdentity userIdentity = _identityService.newUserIdentity(subject,userPrincipal, roles);

        Authentication cached=new SessionAuthentication(getAuthMethod(), userIdentity, null);
        session.setAttribute(SessionAuthentication.__J_AUTHENTICATED, cached);
      }
    }

    return authenticator.validateRequest(req, res, mandatory);
  }
}
