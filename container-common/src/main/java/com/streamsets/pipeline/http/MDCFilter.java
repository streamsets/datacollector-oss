/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.http;

import com.streamsets.pipeline.lib.log.LogConstants;
import org.slf4j.MDC;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.Principal;

public class MDCFilter implements Filter {

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
      ServletException {
    try {
      Principal principal = ((HttpServletRequest)request).getUserPrincipal();
      if (principal != null) {
        MDC.put(LogConstants.USER, principal.getName());
      } else {
        MDC.put(LogConstants.USER, "?");
      }
      chain.doFilter(request, response);
    } finally {
      MDC.clear();
    }
  }

  @Override
  public void destroy() {
  }

}
