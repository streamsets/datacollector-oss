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
package com.streamsets.pipeline.solr.impl;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.security.Principal;
import java.util.Locale;

public class SdcKrb5HttpClientConfigurer extends HttpClientConfigurer {
  // Change for SDC-2962
  //public static final String LOGIN_CONFIG_PROP = "java.security.auth.login.config";
  private static final Logger logger = LoggerFactory.getLogger(HttpClientUtil.class);
  // Change for SDC-2962
  //private static final org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer.ZKJaasConfiguration jaasConf = new org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer.ZKJaasConfiguration();

  public SdcKrb5HttpClientConfigurer() {
  }

  protected void configure(DefaultHttpClient httpClient, SolrParams config) {
    super.configure(httpClient, config);
    // Begin change for SDC-2962
    // Instead of checking existence of JAAS file, do the following if solr kerberos is enabled
    //if(System.getProperty("java.security.auth.login.config") != null) {
      String basicAuthUser = config.get("httpBasicAuthUser");
      String basicAuthPass = config.get("httpBasicAuthPassword");
      if(basicAuthUser != null && basicAuthPass != null) {
        logger.warn("Setting both SPNego auth and basic auth not supported.  Preferring SPNego auth, ignoring basic auth.");
        httpClient.getCredentialsProvider().clear();
      }

      setSPNegoAuth(httpClient);
    //}

  }

  public static boolean setSPNegoAuth(DefaultHttpClient httpClient) {
    // Begin change for SDC-2962
    // Instead of checking existence of JAAS file, do the following if solr kerberos is enabled
    //String configValue = System.getProperty("java.security.auth.login.config");
    //if(configValue != null) {
      //logger.info("Setting up SPNego auth with config: " + configValue);
    // End change for SDC-2962
      String useSubjectCredsProp = "javax.security.auth.useSubjectCredsOnly";
      String useSubjectCredsVal = System.getProperty("javax.security.auth.useSubjectCredsOnly");
      if(useSubjectCredsVal == null) {
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      } else if(!useSubjectCredsVal.toLowerCase(Locale.ROOT).equals("false")) {
        logger.warn("System Property: javax.security.auth.useSubjectCredsOnly set to: " + useSubjectCredsVal + " not false.  SPNego authentication may not be successful.");
      }

      // Change for SDC-2962
      //Configuration.setConfiguration(jaasConf);
      httpClient.getAuthSchemes().register("negotiate", new SPNegoSchemeFactory(true));
      Credentials use_jaas_creds = new Credentials() {
        public String getPassword() {
          return null;
        }

        public Principal getUserPrincipal() {
          return null;
        }
      };
      httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, use_jaas_creds);
      return true;
    /*} else {
      httpClient.getCredentialsProvider().clear();
      return false;
    }*/
  }

  private static class ZKJaasConfiguration extends Configuration {
    private Configuration baseConfig;
    private String zkClientLoginContext;
    // Change for SDC-2962
    //private Set<String> initiateAppNames = new HashSet(Arrays.asList(new String[]{"com.sun.security.jgss.krb5.initiate", "com.sun.security.jgss.initiate"}));

    public ZKJaasConfiguration() {
      try {
        this.baseConfig = Configuration.getConfiguration();
      } catch (SecurityException var2) {
        this.baseConfig = null;
      }

      this.zkClientLoginContext = System.getProperty("zookeeper.sasl.clientconfig", "Client");
      logger.debug("ZK client login context is: " + this.zkClientLoginContext);
    }

    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      // Begin change for SDC-2962
      // Instead of using Jaas file config, use sdc set kerberos
      return Configuration.getConfiguration().getAppConfigurationEntry(appName);
      // End change for SDC-2962
      /*if(this.baseConfig == null) {
        return null;
      } else if(this.initiateAppNames.contains(appName)) {
        logger.debug("Using AppConfigurationEntry for appName: " + this.zkClientLoginContext + " instead of: " + appName);
        return this.baseConfig.getAppConfigurationEntry(this.zkClientLoginContext);
      } else {
        return this.baseConfig.getAppConfigurationEntry(appName);
      }*/
    }
  }
}
