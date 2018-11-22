/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.solr.impl;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthSchemeRegistry;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrPortAwareCookieSpecFactory;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Locale;

public class SdcKrb5HttpClientConfigurer extends SolrHttpClientBuilder {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void configure(DefaultHttpClient httpClient, SolrParams config) {

    final String useSubjectCredsProp = "javax.security.auth.useSubjectCredsOnly";
    String useSubjectCredsVal = System.getProperty(useSubjectCredsProp);

    if (useSubjectCredsVal == null) {
      System.setProperty(useSubjectCredsProp, "false");
    } else if (!useSubjectCredsVal.toLowerCase(Locale.ROOT).equals("false")) {
      logger.warn("System Property: " +
          useSubjectCredsProp +
          " set to: " +
          useSubjectCredsVal +
          " not false. SPNego authentication may not be successful.");
    }

    AuthSchemeRegistry registry = new AuthSchemeRegistry();
    registry.register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, false));
    httpClient.setAuthSchemes(registry);
    // Get the credentials from the JAAS configuration rather than here
    Credentials useJaasCreds = new Credentials() {
      public String getPassword() {
        return null;
      }

      public Principal getUserPrincipal() {
        return null;
      }
    };

    SolrPortAwareCookieSpecFactory cookieFactory = new SolrPortAwareCookieSpecFactory();
    httpClient.getCookieSpecs().register(SolrPortAwareCookieSpecFactory.POLICY_NAME, cookieFactory);
    httpClient.getParams().setParameter(ClientPNames.COOKIE_POLICY, SolrPortAwareCookieSpecFactory.POLICY_NAME);

    httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, useJaasCreds);

    httpClient.addRequestInterceptor(bufferedEntityInterceptor);
  }

  // Set a buffered entity based request interceptor
  private HttpRequestInterceptor bufferedEntityInterceptor = new HttpRequestInterceptor() {
    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
      if (request instanceof HttpEntityEnclosingRequest) {
        HttpEntityEnclosingRequest enclosingRequest = ((HttpEntityEnclosingRequest) request);
        HttpEntity requestEntity = enclosingRequest.getEntity();
        enclosingRequest.setEntity(new BufferedHttpEntity(requestEntity));
      }
    }
  };
}
