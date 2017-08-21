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
package com.streamsets.datacollector.http;

import com.google.common.io.Resources;
import com.streamsets.datacollector.main.MainStandalonePipelineManagerModule;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.testing.NetworkUtils;
import dagger.ObjectGraph;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.UUID;


public class LdapAuthenticationBaseIT {

  static final int LDAP_PORT = 389;

  private static Logger LOG = LoggerFactory.getLogger(LdapAuthenticationBaseIT.class);

  // default bindDn and password for the docker osixia/openldap
  static final String BIND_DN = "cn=admin,dc=example,dc=org";
  static final String BIND_PWD = "admin";

  static Task server;
  static String sdcURL;
  private static String baseDir = "target/" + UUID.randomUUID().toString();
  static final String confDir = baseDir + "/conf";
  private static String dataDir = baseDir + "/data";


  static void startSDCServer(String ldapConf)  throws Exception {
    int port = NetworkUtils.getRandomPort();
    Configuration conf = new Configuration();
    conf.set(WebServerTask.HTTP_PORT_KEY, port);
    conf.set(WebServerTask.AUTHENTICATION_KEY, "basic");
    conf.set(WebServerTask.HTTP_AUTHENTICATION_LOGIN_MODULE, "ldap");
    conf.set(WebServerTask.HTTP_AUTHENTICATION_LDAP_ROLE_MAPPING,
        "managers:manager;engineering:creator;finance:admin;test:admin");
    Writer writer;

    writer = new FileWriter(new File(confDir, "sdc.properties"));
    conf.save(writer);
    writer.close();

    File realmFile = new File(confDir, "ldap-login.conf");
    writer = new FileWriter(realmFile);
    writer.write(ldapConf);
    writer.close();

    // Start SDC
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, confDir);
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, dataDir);
    ObjectGraph dagger = ObjectGraph.create(MainStandalonePipelineManagerModule.class);
    server = dagger.get(TaskWrapper.class);
    server.init();
    server.run();
    sdcURL = "http://localhost:" + Integer.toString(port);
    LOG.debug("server={}", sdcURL);
  }


  static void stopSDCServer() {
    if (server != null) {
      server.stop();
    }
  }

  static LdapConnection setupLdapServer(GenericContainer server, String setupFile) {
    // setup Ldap server 1
    LdapConnection connection = new LdapNetworkConnection(server.getContainerIpAddress(), server.getMappedPort(LDAP_PORT));
    try {
      connection.bind(BIND_DN, BIND_PWD);
      LdifReader reader = new LdifReader(Resources.getResource(setupFile).getFile());
      for (LdifEntry entry : reader) {
        connection.add(entry.getEntry());
      }
    } catch (LdapException e) {
      LOG.error("Setup server 1 failed " + e);
    }
    return connection;
  }
}
