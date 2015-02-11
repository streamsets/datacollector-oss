package com.streamsets.pipeline.http;

import org.eclipse.jetty.security.authentication.FormAuthenticator;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

public class LoginServlet extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
    throws ServletException, IOException {
    doPost(req, resp);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
    throws ServletException, IOException {
    String user = req.getParameter("j_username");
    String pass = req.getParameter("j_password");

    HttpSession session = req.getSession();
    String redirectURL = (String)session.getAttribute(FormAuthenticator.__J_URI);
    if(redirectURL != null && redirectURL.contains("rest/v1/")) {
      req.getSession().setAttribute(FormAuthenticator.__J_URI, "/");
    }

    resp.sendRedirect("j_security_check?j_username="+user+"&j_password="+pass);
  }
}