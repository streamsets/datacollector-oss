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
package com.streamsets.datacollector.publicrestapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.streamsets.datacollector.event.handler.remote.RemoteEventHandlerTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.ConfigurationTestInjector;
import com.streamsets.datacollector.restapi.RuntimeInfoTestInjector;
import com.streamsets.datacollector.restapi.StageLibraryResource;
import com.streamsets.datacollector.restapi.StageLibraryResourceConfig;
import com.streamsets.datacollector.restapi.StartupAuthorizationFeature;
import com.streamsets.datacollector.restapi.WebServerAgentCondition;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.CredentialDeploymentResponseJson;
import com.streamsets.lib.security.http.CredentialDeploymentStatus;
import com.streamsets.lib.security.http.CredentialsBeanJson;
import com.streamsets.lib.security.http.RemoteSSOService;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

import static com.streamsets.datacollector.publicrestapi.CredentialsDeploymentResource.DPM_AGENT_PUBLIC_KEY;

public class TestCredentialsDeploymentResource extends JerseyTest{

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Before
  public void setUp() throws Exception {
    WebServerAgentCondition.reset();
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    System.clearProperty("streamsets.cluster.manager.credentials.required");
    System.clearProperty(DPM_AGENT_PUBLIC_KEY);
    super.tearDown();
  }

  @Test
  public void testNonRunningRestAPI() {
    Response response = null;
    System.setProperty("streamsets.cluster.manager.credentials.required", "true");
    try {
      response = target("/v1/definitions").request().get();
      Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  @Test
  public void testSuccess() throws Exception {
    Properties sdcProps = new Properties();
    sdcProps.setProperty("a", "b");
    sdcProps.setProperty("c", "d");
    sdcProps.setProperty("kerberos.client.keytab", "sdc.keytab");
    sdcProps.setProperty("kerberos.client.enabled", "false");
    sdcProps.setProperty("kerberos.client.principal", "sdc/_HOST@EXAMPLE.COM");
    File sdcFile = new File(RuntimeInfoTestInjector.confDir, "sdc.properties");

    Properties dpmProps = new Properties();
    dpmProps.setProperty("x", "y");
    dpmProps.setProperty("z", "a");
    dpmProps.setProperty("dpm.enabled", "false");
    dpmProps.setProperty("dpm.base.url", "http://localhost:18631");
    File dpmFile = new File(RuntimeInfoTestInjector.confDir, "dpm.properties");

    try (FileWriter fw = new FileWriter(sdcFile)) {
      sdcProps.store(fw, "");
    }

    try (FileWriter fw = new FileWriter(dpmFile)) {
      dpmProps.store(fw, "");
    }

    Response response = null;
    KeyPair keys = generateKeys();
    System.setProperty("streamsets.cluster.manager.credentials.required", "true");
    System.setProperty(DPM_AGENT_PUBLIC_KEY, Base64.getEncoder().encodeToString(keys.getPublic().getEncoded()));
    String token = "Frenchies and Pandas";
    Signature sig = Signature.getInstance("SHA256withRSA");
    sig.initSign(keys.getPrivate());
    sig.update(token.getBytes(Charsets.UTF_8));
    List<String> labels = Arrays.asList("deployment-prod-1", "deployment-prod-2");
    CredentialsBeanJson json =
        new CredentialsBeanJson(token,
            "streamsets/172.1.1.0@EXAMPLE.COM",
            Base64.getEncoder().encodeToString("testKeytab".getBytes(Charsets.UTF_8)),
            Base64.getEncoder().encodeToString(sig.sign()),
            "https://dpm.streamsets.com:18631",
            Arrays.asList("deployment-prod-1", "deployment-prod-2"),
            "deployment1:org"
        );

    try {
      response = target("/v1/deployment/deployCredentials").request().post(Entity.json(json));
      Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      CredentialDeploymentResponseJson responseJson =
          OBJECT_MAPPER.readValue((InputStream) response.getEntity(), CredentialDeploymentResponseJson.class);
      Assert.assertEquals(
          CredentialDeploymentStatus.CREDENTIAL_USED_AND_DEPLOYED,
          responseJson.getCredentialDeploymentStatus()
      );

      // Verify sdc.properties
      sdcProps = new Properties();
      try (FileReader fr = new FileReader(sdcFile)) {
        sdcProps.load(fr);
      }
      Assert.assertEquals("b", sdcProps.getProperty("a"));
      Assert.assertEquals("d", sdcProps.getProperty("c"));
      Assert.assertEquals("streamsets/172.1.1.0@EXAMPLE.COM", sdcProps.getProperty("kerberos.client.principal"));
      Assert.assertEquals("true", sdcProps.getProperty("kerberos.client.enabled"));
      Assert.assertEquals("sdc.keytab", sdcProps.getProperty("kerberos.client.keytab"));
      byte[] keyTab = Files.toByteArray(new File(RuntimeInfoTestInjector.confDir, "sdc.keytab"));
      Assert.assertEquals("testKeytab", new String(keyTab, Charsets.UTF_8));
      response = target("/v1/definitions").request().get();
      Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

      dpmProps = new Properties();
      try (FileReader fr = new FileReader(dpmFile)) {
        dpmProps.load(fr);
      }
      Assert.assertEquals("y", dpmProps.getProperty("x"));
      Assert.assertEquals("a", dpmProps.getProperty("z"));
      Assert.assertEquals("true", dpmProps.getProperty("dpm.enabled"));
      Assert.assertEquals(Configuration.FileRef.PREFIX + "application-token.txt" + Configuration.FileRef.SUFFIX,
          dpmProps.getProperty("dpm.appAuthToken"));
      Assert.assertEquals("https://dpm.streamsets.com:18631", dpmProps.getProperty("dpm.base.url"));

      Assert.assertEquals(StringUtils.join(labels.toArray(), ","), dpmProps.getProperty(RemoteEventHandlerTask
          .REMOTE_JOB_LABELS));
      Assert.assertEquals("deployment1:org", dpmProps.getProperty(RemoteSSOService.DPM_DEPLOYMENT_ID));


      File tokenFile = new File(RuntimeInfoTestInjector.confDir, "application-token.txt");
      try (FileInputStream fr = new FileInputStream(tokenFile)) {
        int len = token.length();
        byte[] tokenBytes = new byte[len];
        Assert.assertEquals(len, fr.read(tokenBytes));
        Assert.assertEquals(token, new String(tokenBytes, Charsets.UTF_8));
      }
      //Test redeploying the credentials again
      response = target("/v1/deployment/deployCredentials").request().post(Entity.json(json));
      responseJson =
          OBJECT_MAPPER.readValue((InputStream) response.getEntity(), CredentialDeploymentResponseJson.class);
      Assert.assertEquals(
          CredentialDeploymentStatus.CREDENTIAL_NOT_USED_ALREADY_DEPLOYED,
          responseJson.getCredentialDeploymentStatus()
      );

    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  @Test
  public void testResourcesThatDontNeedCredentials() throws Exception {
    Response response = null;
    System.setProperty("streamsets.cluster.manager.credentials.required", "true");

    try {
      response = target("/v1/cluster/callback").request().get();
      // It can be anything, but not 403
      Assert.assertNotEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }


  private KeyPair generateKeys() throws Exception {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
    keyGen.initialize(512, random);
    return keyGen.generateKeyPair();
  }

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(StartupAuthorizationFeature.class);
        register(new CredentialsDeploymentResourceConfig());
        register(CredentialsDeploymentResource.class);
        register(new StageLibraryResourceConfig());
        register(StageLibraryResource.class);
        register(MultiPartFeature.class);
        register(PublicClusterResource.class);
      }
    };
  }

  static class CredentialsDeploymentResourceConfig extends AbstractBinder {
    @Override
    protected void configure() {
      bindFactory(RuntimeInfoTestInjector.class).to(RuntimeInfo.class);
      bindFactory(ConfigurationTestInjector.class).to(Configuration.class);
    }
  }

}
