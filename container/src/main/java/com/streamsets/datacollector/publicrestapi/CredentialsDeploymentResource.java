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

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.event.handler.remote.RemoteEventHandlerTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.WebServerAgentCondition;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.CredentialDeploymentResponseJson;
import com.streamsets.lib.security.http.CredentialDeploymentStatus;
import com.streamsets.lib.security.http.CredentialsBeanJson;
import com.streamsets.lib.security.http.RemoteSSOService;
import io.swagger.annotations.Api;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

@Path("/v1/deployment")
@Api(value = "deployment")
@PermitAll
public class CredentialsDeploymentResource {
  private static final Logger LOG = LoggerFactory.getLogger(CredentialsDeploymentResource.class);
  private static final String APPLICATION_TOKEN_TXT = "application-token.txt";
  private static final int MAX_FAILURES_ALLOWED = 100;

  static final String DPM_AGENT_PUBLIC_KEY = "streamsets.cluster.manager.public.key";
  private final RuntimeInfo runtimeInfo;
  private final AtomicInteger failedCount = new AtomicInteger(0);

  @Inject
  public CredentialsDeploymentResource(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/deployCredentials")
  public Response deployCredentials(CredentialsBeanJson credentialsBeanJson) throws Exception {
    LOG.info("Credentials have been received. Validating..");
    boolean isValid = validateSignature(credentialsBeanJson);
    CredentialDeploymentStatus credentialDeploymentStatus;
    if (isValid) {
      if (!WebServerAgentCondition.getReceivedCredentials()) {
        deployDPMToken(credentialsBeanJson);
        handleKerberos(credentialsBeanJson);
        WebServerAgentCondition.setCredentialsReceived();
        credentialDeploymentStatus = CredentialDeploymentStatus.CREDENTIAL_USED_AND_DEPLOYED;
      } else {
        LOG.info("Credentials already received, so not using the token");
        credentialDeploymentStatus = CredentialDeploymentStatus.CREDENTIAL_NOT_USED_ALREADY_DEPLOYED;
      }
      return Response.ok(new CredentialDeploymentResponseJson(runtimeInfo.getId(), credentialDeploymentStatus)).build();
    } else {
      LOG.warn("Received credentials were invalid, {} of maximum {} attempts",
          failedCount.incrementAndGet(), MAX_FAILURES_ALLOWED);
      if (failedCount.get() > MAX_FAILURES_ALLOWED) {
        LOG.error("Failed to validate Cluster Manager credentials 100 times, " +
            "likely due to agent failure or a denial of service attack");
        System.exit(-1);
      }
      return Response.status(Response.Status.BAD_REQUEST).entity("Cannot validate the received credentials").build();
    }
  }

  private boolean validateSignature(CredentialsBeanJson credentialsBeanJson)
      throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
    // getProperty so we can test it
    String publicKey = Preconditions.checkNotNull(System.getProperty(DPM_AGENT_PUBLIC_KEY));

    X509EncodedKeySpec kspec = new X509EncodedKeySpec(Base64.getDecoder().decode(publicKey));
    KeyFactory kf = KeyFactory.getInstance("RSA");
    PublicKey key = kf.generatePublic(kspec);
    Signature sig = Signature.getInstance("SHA256withRSA");
    sig.initVerify(key);
    sig.update(credentialsBeanJson.getToken().getBytes(Charsets.UTF_8));
    LOG.info("Token : {}, Signature {}", credentialsBeanJson.getToken(), credentialsBeanJson.getTokenSignature());
    return sig.verify(Base64.getDecoder().decode(credentialsBeanJson.getTokenSignature()));
  }

  private void handleKerberos(CredentialsBeanJson credentialsBeanJson) throws IOException {
    if (!StringUtils.isEmpty(credentialsBeanJson.getPrincipal())) {
      LOG.info("Kerberos credentials found, deploying..");
      byte[] decodedKeytab = Base64.getDecoder().decode(credentialsBeanJson.getKeytab());
      java.nio.file.Path keytab = Paths.get(runtimeInfo.getConfigDir(), "sdc.keytab");
      Files.write(keytab, decodedKeytab, CREATE, WRITE);
      File sdcProperties = new File(runtimeInfo.getConfigDir(), "sdc.properties");
      Configuration conf = new Configuration();
      try (FileReader reader = new FileReader(sdcProperties)) {
        conf.load(reader);
      }
      conf.set("kerberos.client.principal", credentialsBeanJson.getPrincipal());
      conf.set("kerberos.client.enabled", true);
      conf.set("kerberos.client.keytab", "sdc.keytab");
      try (FileWriter writer = new FileWriter(sdcProperties)) {
        conf.save(writer);
      }
      LOG.info("Kerberos credentials deployed.");
    }
  }

  private void deployDPMToken(CredentialsBeanJson credentialsBeanJson) throws IOException {
    LOG.info("Deploying DPM token");
    File dpmProperties = new File(runtimeInfo.getConfigDir(), "dpm.properties");
    Configuration conf = new Configuration();
    Files.write(Paths.get(runtimeInfo.getConfigDir(), "application-token.txt"),
        credentialsBeanJson.getToken().getBytes(Charsets.UTF_8), CREATE, WRITE);
    try (FileReader reader = new FileReader(dpmProperties)) {
      conf.load(reader);
    }

    conf.unset(RemoteSSOService.DPM_BASE_URL_CONFIG);
    conf.set(RemoteSSOService.DPM_ENABLED, true);

    conf.set(
        RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG,
        Configuration.FileRef.PREFIX + APPLICATION_TOKEN_TXT + Configuration.FileRef.SUFFIX
    );

    conf.set(RemoteSSOService.DPM_DEPLOYMENT_ID, credentialsBeanJson.getDeploymentId());
    runtimeInfo.setDeploymentId(credentialsBeanJson.getDeploymentId());

    if(!CollectionUtils.isEmpty(credentialsBeanJson.getLabels())) {
      String labelsString = StringUtils.join(credentialsBeanJson.getLabels().toArray(), ",");
      LOG.info("SDC will have the following Labels: {}", labelsString);
      conf.set(RemoteEventHandlerTask.REMOTE_JOB_LABELS, labelsString);
    }
    try (FileWriter writer = new FileWriter(dpmProperties)) {
      conf.save(writer);
    }
    Files.write(
        Paths.get(dpmProperties.getPath()) ,
        (RemoteSSOService.DPM_BASE_URL_CONFIG + "=" + credentialsBeanJson.getDpmUrl()).getBytes(),
        StandardOpenOption.APPEND
    );
    runtimeInfo.setDPMEnabled(true);
    LOG.info("DPM token deployed");
  }
}
