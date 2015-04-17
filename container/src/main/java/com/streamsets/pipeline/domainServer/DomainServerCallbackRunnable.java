/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.domainServer;

import com.streamsets.pipeline.main.RuntimeInfo;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DomainServerCallbackRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DomainServerCallbackRunnable.class);
  public static final String RUNNABLE_NAME = "DomainServerCallbackRunnable";
  private final RuntimeInfo runtimeInfo;
  private final String domainServerURL;

  public DomainServerCallbackRunnable(RuntimeInfo runtimeInfo, String domainServerURL) {
    this.runtimeInfo = runtimeInfo;
    this.domainServerURL = domainServerURL;
  }

  @Override
  public void run() {
    String originalName = Thread.currentThread().getName();
    Thread.currentThread().setName(originalName + "-" + RUNNABLE_NAME);
    try {
      HttpClient httpClient = new HttpClient();
      httpClient.start();
      Request request = httpClient.newRequest(domainServerURL)
        .method(HttpMethod.POST);

      Map<String, String> authenticationToken = runtimeInfo.getAuthenticationTokens();
      for(String role: authenticationToken.keySet()) {
        request.param(role, authenticationToken.get(role));
      }

      request.send(new Response.CompleteListener() {
        @Override
        public void onComplete(Result result) {
          Response response = result.getResponse();

          if(response.getStatus() != 200) {
            LOG.warn("Failed to ping Domain Server with HTTP Status - {}", response.getStatus());
          }
        }
      });

    } catch (Exception ex) {
      LOG.warn("Error while calling callback to Domain Server , {}", ex.getMessage(), ex);
    } finally {
      Thread.currentThread().setName(originalName);
    }
  }
}
