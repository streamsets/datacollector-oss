/*
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.http;

import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.client.oauth1.AccessToken;
import org.glassfish.jersey.client.oauth1.ConsumerCredentials;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Feature;

/**
 * Reusable methods for configuring a jersey http client.
 */
public class JerseyClientUtil {
  private static final Logger LOG = LoggerFactory.getLogger(JerseyClientUtil.class);

  private JerseyClientUtil() {}

  public static void configurePasswordAuth(
      AuthenticationType authType,
      PasswordAuthConfigBean conf,
      ClientBuilder clientBuilder
  ) {
    if (authType == AuthenticationType.BASIC) {
      clientBuilder.register(HttpAuthenticationFeature.basic(conf.username, conf.password));
    }

    if (authType == AuthenticationType.DIGEST) {
      clientBuilder.register(HttpAuthenticationFeature.digest(conf.username, conf.password));
    }

    if (authType == AuthenticationType.UNIVERSAL) {
      clientBuilder.register(HttpAuthenticationFeature.universal(conf.username, conf.password));
    }
  }

  public static AccessToken configureOAuth1(OAuthConfigBean conf, ClientBuilder clientBuilder) {
    ConsumerCredentials consumerCredentials = new ConsumerCredentials(conf.consumerKey, conf.consumerSecret);

    AccessToken accessToken = new AccessToken(conf.token, conf.tokenSecret);
    Feature feature = OAuth1ClientSupport.builder(consumerCredentials)
        .feature()
        .accessToken(accessToken)
        .build();
    clientBuilder.register(feature);

    return accessToken;
  }

  public static ClientBuilder configureSslContext(SslConfigBean conf, ClientBuilder clientBuilder) {

    SslConfigurator sslConfig = SslConfigurator.newInstance();

    if (!conf.trustStorePath.isEmpty() && !conf.trustStorePassword.isEmpty()) {
      sslConfig.trustStoreFile(conf.trustStorePath).trustStorePassword(conf.trustStorePassword);
    }

    if (!conf.keyStorePath.isEmpty() && !conf.keyStorePassword.isEmpty()) {
      sslConfig.keyStoreFile(conf.keyStorePath).keyStorePassword(conf.keyStorePassword);
    }

    SSLContext sslContext = sslConfig.createSSLContext();
    return clientBuilder.sslContext(sslContext);
  }

  public static ClientBuilder configureProxy(HttpProxyConfigBean conf, ClientBuilder clientBuilder) {
    if (!conf.uri.isEmpty()) {
      clientBuilder.property(ClientProperties.PROXY_URI, conf.uri);
      LOG.debug("Using Proxy: '{}'", conf.uri);
    }
    if (!conf.username.isEmpty()) {
      clientBuilder.property(ClientProperties.PROXY_USERNAME, conf.username);
      LOG.debug("Using Proxy Username: '{}'", conf.username);
    }
    if (!conf.password.isEmpty()) {
      clientBuilder.property(ClientProperties.PROXY_PASSWORD, conf.password);
      LOG.debug("Using Proxy Password: '{}'", conf.password);
    }

    return clientBuilder;
  }
}
