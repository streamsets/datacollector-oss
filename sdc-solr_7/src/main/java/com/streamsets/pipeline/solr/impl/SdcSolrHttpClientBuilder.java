/*
 * Copyright 2019 StreamSets Inc.
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

import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.cookie.CookieSpecProvider;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrPortAwareCookieSpecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Locale;

public class SdcSolrHttpClientBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final static String FALSE = "false";
  private final static String USE_SUBJECT_CREDENTIALS_PROPERTY = "javax.security.auth.useSubjectCredsOnly";

  static SolrHttpClientBuilder create() {
    SolrHttpClientBuilder solrHttpClientBuilder = SolrHttpClientBuilder.create();

    final String useSubjectCredentialsProperty = USE_SUBJECT_CREDENTIALS_PROPERTY;
    String useSubjectCredentialsValue = System.getProperty(useSubjectCredentialsProperty);

    if (useSubjectCredentialsValue == null) {
      System.setProperty(useSubjectCredentialsProperty, FALSE);
    } else if (!useSubjectCredentialsValue.toLowerCase(Locale.ROOT).equals(FALSE)) {
      LOG.warn(String.format(
          "System Property: %s set to: %s not false. SPNego authentication may not be successful.",
          useSubjectCredentialsProperty,
          useSubjectCredentialsValue
      ));
    }

    solrHttpClientBuilder.setAuthSchemeRegistryProvider(() -> RegistryBuilder.<AuthSchemeProvider>create().register(AuthSchemes.SPNEGO,
        new SPNegoSchemeFactory(true)
    ).build());

    SolrPortAwareCookieSpecFactory cookieFactory = new SolrPortAwareCookieSpecFactory();
    solrHttpClientBuilder.setCookieSpecRegistryProvider(() -> RegistryBuilder.<CookieSpecProvider>create().register(SolrPortAwareCookieSpecFactory.POLICY_NAME,
        cookieFactory
    ).build());

    Credentials jassCredentials = new Credentials() {
      public String getPassword() {
        return null;
      }

      public Principal getUserPrincipal() {
        return null;
      }
    };

    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, jassCredentials);
    solrHttpClientBuilder.setDefaultCredentialsProvider(() -> credentialsProvider);

    return solrHttpClientBuilder;
  }
}
