/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.lib.http;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.client.oauth1.AccessToken;
import org.glassfish.jersey.client.oauth1.ConsumerCredentials;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Feature;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reusable methods for configuring a jersey http client.
 */
public class JerseyClientUtil {
  private static final Logger LOG = LoggerFactory.getLogger(JerseyClientUtil.class);

  private JerseyClientUtil() {}

  public static void configurePasswordAuth(
      AuthenticationType authType,
      String username,
      String password,
      ClientBuilder clientBuilder
  ) {
    if (authType == AuthenticationType.BASIC) {
      clientBuilder.register(HttpAuthenticationFeature.basic(username, password));
    }

    if (authType == AuthenticationType.DIGEST) {
      clientBuilder.register(HttpAuthenticationFeature.digest(username, password));
    }

    if (authType == AuthenticationType.UNIVERSAL) {
      clientBuilder.register(HttpAuthenticationFeature.universal(username, password));
    }
  }

  public static AccessToken configureOAuth1(
    String consumerKey,
    String consumerSecret,
    String token,
    String tokenSecret,
    ClientBuilder clientBuilder
  ) {
    ConsumerCredentials consumerCredentials = new ConsumerCredentials(consumerKey, consumerSecret);

    AccessToken accessToken = new AccessToken(token, tokenSecret);
    Feature feature = OAuth1ClientSupport.builder(consumerCredentials)
        .feature()
        .accessToken(accessToken)
        .build();
    clientBuilder.register(feature);

    return accessToken;
  }

  public static ClientBuilder configureSslContext(TlsConfigBean conf, ClientBuilder clientBuilder) {
    SSLContext sslContext = conf.getSslContext();
    if (sslContext == null) {
      return clientBuilder;
    } else {
      return clientBuilder.sslContext(sslContext);
    }
  }

  public static ClientBuilder configureProxy(
    String uri,
    String username,
    String password,
    ClientBuilder clientBuilder
  ) {
    if (!StringUtils.isEmpty(uri)) {
      clientBuilder.property(ClientProperties.PROXY_URI, uri);
      LOG.debug("Using Proxy: '{}'", uri);
    } else { // No proxy URI, then return
      return clientBuilder;
    }
    if (!StringUtils.isEmpty(username)) {
      clientBuilder.property(ClientProperties.PROXY_USERNAME, username);
      LOG.debug("Using Proxy Username: '{}'", username);
    }
    if (!StringUtils.isEmpty(password)) {
      clientBuilder.property(ClientProperties.PROXY_PASSWORD, password);
      LOG.debug("Using Proxy Password: '{}'", password);
    }

    return clientBuilder;
  }

  /**
   * Helper method to upgrade both HTTP stages to the JerseyConfigBean
   * @param configs List of configs to upgrade.
   */
  public static void upgradeToJerseyConfigBean(List<Config> configs) {
    List<Config> configsToAdd = new ArrayList<>();
    List<Config> configsToRemove = new ArrayList<>();
    List<String> movedConfigs = ImmutableList.of(
        "conf.requestTimeoutMillis",
        "conf.numThreads",
        "conf.authType",
        "conf.oauth",
        "conf.basicAuth",
        "conf.useProxy",
        "conf.proxy",
        "conf.sslConfig"
    );
    for (Config config : configs) {
      if (hasPrefixIn(movedConfigs, config.getName())) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(config.getName().replace("conf.", "conf.client."), config.getValue()));
      }
    }

    configsToAdd.add(new Config("conf.client.transferEncoding", RequestEntityProcessing.CHUNKED));

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

  private static boolean hasPrefixIn(List<String> movedConfigs, String name) {
    checkNotNull(name, "Config name cannot be null.");
    for (String config : movedConfigs) {
      if (name.startsWith(config)) {
        return true;
      }
    }
    return false;
  }
}
