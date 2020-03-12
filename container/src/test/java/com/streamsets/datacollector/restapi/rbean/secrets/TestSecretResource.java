/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.restapi.rbean.secrets;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.TestUtil;
import com.streamsets.datacollector.restapi.configuration.ExceptionToHttpErrorProvider;
import com.streamsets.datacollector.restapi.rbean.lang.REnum;
import com.streamsets.datacollector.restapi.rbean.lang.RString;
import com.streamsets.datacollector.restapi.rbean.rest.OkPaginationRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.OkRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.PaginationInfoInjectorBinder;
import com.streamsets.datacollector.restapi.rbean.rest.RestRequest;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TestSecretResource extends JerseyTest {

  @Override
  protected TestContainerFactory getTestContainerFactory() {
    return new GrizzlyWebTestContainerFactory();
  }

  @Override
  protected DeploymentContext configureDeployment() {
    forceSet(TestProperties.CONTAINER_PORT, "0");
    return ServletDeploymentContext.forServlet(new ServletContainer(new SecretResourceTestConfig())).build();
  }

  private static class SecretResourceTestConfig extends ResourceConfig {
    SecretResourceTestConfig() {
      register(new PaginationInfoInjectorBinder());
      register(MultiPartFeature.class);
      register(SecretResource.class);
      register(new ExceptionToHttpErrorProvider());
      register(new AbstractBinder() {
        @Override
        protected void configure() {
          bindFactory(TestUtil.CredentialStoreTaskTestInjector.class).to(CredentialStoresTask.class);
          bindFactory(ConfigurationTestInjector.class).to(Configuration.class);
        }
      });
      register(JacksonObjectMapperResolver.class);
    }
  }


  private static class ConfigurationTestInjector implements Factory<Configuration> {
    @Override
    public Configuration provide() {
      Configuration configuration = new Configuration();
      configuration.set(CredentialStoresTask.MANAGED_DEFAULT_CREDENTIAL_STORE_CONFIG, "streamsets");
      configuration.set(SecretResource.MAX_FILE_SIZE_KB_LIMIT, SecretResource.MAX_FILE_SIZE_KB_LIMIT_DEFAULT);
      return configuration;
    }

    @Override
    public void dispose(Configuration configuration) {

    }
  }

  @Provider
  private static class JacksonObjectMapperResolver implements ContextResolver<ObjectMapper> {
    @Override
    public ObjectMapper getContext(Class<?> type) {
      return ObjectMapperFactory.get();
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    TestUtil.CredentialStoreTaskTestInjector.INSTANCE.destroy();
  }

  @Test
  public void createSecret() throws Exception {
    String pipelineId = "PIPELINE_VAULT_pipelineId";
    String secretName = "stage_id_config1";
    String secretValue = "secret";

    RSecret rSecret = new RSecret();
    rSecret.setVault(new RString(pipelineId));
    rSecret.setName(new RString(secretName));
    rSecret.setType(new REnum<SecretType>().setValue(SecretType.TEXT));
    rSecret.setValue(new RString(secretValue));

    RestRequest<RSecret> restRequest = new RestRequest<>();
    restRequest.setData(rSecret);

    Response response = target("/v1/secrets/text/ctx=SecretManage")
        .request()
        .post(Entity.entity(ObjectMapperFactory.get().writeValueAsString(restRequest), MediaType.APPLICATION_JSON_TYPE));

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    InputStream responseEntity = (InputStream) response.getEntity();

    OkRestResponse<RSecret> secretResponse = ObjectMapperFactory.get().readValue(
        responseEntity,
        new TypeReference<OkRestResponse<RSecret>>() {}
    );

    rSecret = secretResponse.getData();
    Assert.assertNotNull(rSecret);
    Assert.assertEquals(pipelineId, rSecret.getVault().getValue());
    Assert.assertEquals(secretName, rSecret.getName().getValue());
    Assert.assertTrue(rSecret.getValue().isScrubbed());

    Assert.assertTrue(TestUtil.CredentialStoreTaskTestInjector.INSTANCE.getNames().contains(pipelineId + "/" + secretName));
    CredentialValue value = TestUtil.CredentialStoreTaskTestInjector.INSTANCE.get(
        "",
        pipelineId + "/" + secretName,
        ""
    );
    Assert.assertEquals(secretValue, value.get());
  }

  @Test
  public void createFileSecret() throws Exception {
    String pipelineId = "PIPELINE_VAULT_pipelineId";
    String secretName = "stage_id_config1";
    String secretValue = "secret";


    Path tempFile = Files.createTempFile("secret.txt", ".crt");

    try {
      try (FileWriter f = new FileWriter(tempFile.toFile().getAbsolutePath())) {
        f.write(secretValue);
      }

      FileDataBodyPart fileDataBodyPart = new FileDataBodyPart("uploadedFile", tempFile.toFile());
      FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
      final FormDataMultiPart multipart = (FormDataMultiPart) formDataMultiPart
          .field("vault", pipelineId, MediaType.APPLICATION_JSON_TYPE)
          .field("name", secretName, MediaType.APPLICATION_JSON_TYPE)
          .bodyPart(fileDataBodyPart);

      Response response = target("/v1/secrets/file/ctx=SecretManage").register(MultiPartFeature.class).request().post(
          Entity.entity(multipart, multipart.getMediaType()));

      Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

      InputStream responseEntity = (InputStream) response.getEntity();

      OkRestResponse<RSecret> secretResponse = ObjectMapperFactory.get()
          .readValue(responseEntity, new TypeReference<OkRestResponse<RSecret>>() {});

      RSecret rSecret = secretResponse.getData();
      Assert.assertNotNull(rSecret);
      Assert.assertEquals(pipelineId, rSecret.getVault().getValue());
      Assert.assertEquals(secretName, rSecret.getName().getValue());

      Assert.assertTrue(TestUtil.CredentialStoreTaskTestInjector.INSTANCE.getNames().contains(pipelineId + "/" + secretName));
      CredentialValue value = TestUtil.CredentialStoreTaskTestInjector.INSTANCE.get("",
          pipelineId + "/" + secretName,
          ""
      );
      Assert.assertEquals(IOUtils.toString(new FileReader(tempFile.toFile())), value.get());
    } finally {
      Files.delete(tempFile);
    }
  }

  @Test
  public void createFileSecretFileSizeExceeded() throws Exception {
    String pipelineId = "PIPELINE_VAULT_pipelineId";
    String secretName = "stage_id_config1";
    String secretValue = "secret";

    long maxBytesAllowedInFile = SecretResource.MAX_FILE_SIZE_KB_LIMIT_DEFAULT * 1024L;

    Path tempFile = Files.createTempFile("secret.txt", ".crt");

    try {
      try (FileWriter f = new FileWriter(tempFile.toFile().getAbsolutePath())) {
        int numberOfBytesWritten = 0;
        while (numberOfBytesWritten <= maxBytesAllowedInFile + 1) {
          f.write(secretValue);
          numberOfBytesWritten += secretValue.getBytes().length;
        }
      }

      FileDataBodyPart fileDataBodyPart = new FileDataBodyPart("uploadedFile", tempFile.toFile());
      FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
      final FormDataMultiPart multipart = (FormDataMultiPart) formDataMultiPart
          .field("vault", pipelineId, MediaType.APPLICATION_JSON_TYPE)
          .field("name", secretName, MediaType.APPLICATION_JSON_TYPE)
          .bodyPart(fileDataBodyPart);

      Response response = target("/v1/secrets/file/ctx=SecretManage").register(MultiPartFeature.class).request()
          .post(Entity.entity(multipart, multipart.getMediaType()));

      Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
      Assert.assertFalse(
          TestUtil.CredentialStoreTaskTestInjector.INSTANCE.getNames().contains(pipelineId + "/" + secretName)
      );
    } finally {
      Files.delete(tempFile);
    }
  }


  @Test
  public void updateSecret() throws Exception {
    String pipelineId = "PIPELINE_VAULT_pipelineId";
    String secretName = "stage_id_config1";
    String secretValue = "secretValue";
    TestUtil.CredentialStoreTaskTestInjector.INSTANCE.store(CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST, pipelineId + "/" + secretName, secretValue);
    String changedSecretValue = "changedSecretValue";

    String secretPath = pipelineId + "/" + secretName;

    RSecret rSecret = new RSecret();
    rSecret.setVault(new RString(pipelineId));
    rSecret.setName(new RString(secretName));
    rSecret.setType(new REnum<SecretType>().setValue(SecretType.TEXT));
    rSecret.setValue(new RString(changedSecretValue));

    RestRequest<RSecret> restRequest = new RestRequest<>();
    restRequest.setData(rSecret);

    Response response = target(
        Utils.format(
            "/v1/secrets/{}/text/ctx=SecretManage",
            URLEncoder.encode(secretPath, StandardCharsets.UTF_8.name())
        )
    ).request()
        .post(Entity.entity(ObjectMapperFactory.get().writeValueAsString(restRequest), MediaType.APPLICATION_JSON_TYPE));

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    InputStream responseEntity = (InputStream) response.getEntity();

    OkRestResponse<RSecret> secretResponse = ObjectMapperFactory.get().readValue(
        responseEntity,
        new TypeReference<OkRestResponse<RSecret>>() {}
    );

    rSecret = secretResponse.getData();
    Assert.assertNotNull(rSecret);
    Assert.assertEquals(pipelineId, rSecret.getVault().getValue());
    Assert.assertEquals(secretName, rSecret.getName().getValue());
    Assert.assertTrue(rSecret.getValue().isScrubbed());

    Assert.assertTrue(TestUtil.CredentialStoreTaskTestInjector.INSTANCE.getNames().contains(pipelineId + "/" + secretName));
    CredentialValue value = TestUtil.CredentialStoreTaskTestInjector.INSTANCE.get(
        "",
        pipelineId + "/" + secretName,
        ""
    );
    Assert.assertEquals(changedSecretValue, value.get());

  }

  @Test
  public void updateFileSecret() throws Exception {
    String pipelineId = "PIPELINE_VAULT_pipelineId";
    String secretName = "stage_id_config1";
    String secretValue = "secretValue";
    TestUtil.CredentialStoreTaskTestInjector.INSTANCE.store(
        CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST, pipelineId + "/" + secretName,
        secretValue
    );
    String changedSecretValue = "changedSecretValue";

    String secretPath = pipelineId + "/" + secretName;

    Path tempFile = Files.createTempFile("secret.txt", ".crt");

    try {
      try (FileWriter f = new FileWriter(tempFile.toFile().getAbsolutePath())) {
        f.write(changedSecretValue);
      }

      FileDataBodyPart fileDataBodyPart = new FileDataBodyPart("uploadedFile", tempFile.toFile());
      FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
      final FormDataMultiPart multipart = (FormDataMultiPart) formDataMultiPart
          .field("vault", pipelineId, MediaType.APPLICATION_JSON_TYPE)
          .field("name", secretName, MediaType.APPLICATION_JSON_TYPE)
          .bodyPart(fileDataBodyPart);


      Response response = target(
          Utils.format("/v1/secrets/{}/file/ctx=SecretManage", URLEncoder.encode(secretPath, StandardCharsets.UTF_8.name()))
      ).register(MultiPartFeature.class).request().post(Entity.entity(multipart, multipart.getMediaType()));

      Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

      InputStream responseEntity = (InputStream) response.getEntity();

      OkRestResponse<RSecret> secretResponse = ObjectMapperFactory.get()
          .readValue(responseEntity, new TypeReference<OkRestResponse<RSecret>>() {});

      RSecret rSecret = secretResponse.getData();
      Assert.assertNotNull(rSecret);
      Assert.assertEquals(pipelineId, rSecret.getVault().getValue());
      Assert.assertEquals(secretName, rSecret.getName().getValue());

      Assert.assertTrue(TestUtil.CredentialStoreTaskTestInjector.INSTANCE.getNames().contains(pipelineId + "/" + secretName));
      CredentialValue changedValue = TestUtil.CredentialStoreTaskTestInjector.INSTANCE.get("",
          pipelineId + "/" + secretName,
          ""
      );
      Assert.assertEquals(IOUtils.toString(new FileReader(tempFile.toFile())), changedValue.get());
      Assert.assertEquals(changedSecretValue, changedValue.get());
    } finally {
      Files.delete(tempFile);
    }
  }

  @Test
  public void updateFileSecretFileSizeExceeded() throws Exception {
    String pipelineId = "PIPELINE_VAULT_pipelineId";
    String secretName = "stage_id_config1";
    String secretValue = "secretValue";
    TestUtil.CredentialStoreTaskTestInjector.INSTANCE.store(
        CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST, pipelineId + "/" + secretName,
        secretValue
    );
    String changedSecretValue = "changedSecretValue";

    String secretPath = pipelineId + "/" + secretName;


    Path tempFile = Files.createTempFile("secret.txt", ".crt");
    long maxBytesAllowedInFile = SecretResource.MAX_FILE_SIZE_KB_LIMIT_DEFAULT * 1024L;
    try {
      try (FileWriter f = new FileWriter(tempFile.toFile().getAbsolutePath())) {
        int numberOfBytesWritten = 0;
        while (numberOfBytesWritten <= maxBytesAllowedInFile + 1) {
          f.write(changedSecretValue);
          numberOfBytesWritten += secretValue.getBytes().length;
        }
      }

      FileDataBodyPart fileDataBodyPart = new FileDataBodyPart("uploadedFile", tempFile.toFile());
      FormDataMultiPart formDataMultiPart = new FormDataMultiPart();

      final FormDataMultiPart multipart = (FormDataMultiPart) formDataMultiPart
          .field("vault", pipelineId, MediaType.APPLICATION_JSON_TYPE)
          .field("name", secretName, MediaType.APPLICATION_JSON_TYPE)
          .bodyPart(fileDataBodyPart);

      Response response = target(
          Utils.format("/v1/secrets/{}/file/ctx=SecretManage", URLEncoder.encode(secretPath, StandardCharsets.UTF_8.name()))
      ).register(MultiPartFeature.class).request().post(Entity.entity(multipart, multipart.getMediaType()));

      Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());

      //Max sure the old value is not changed
      Assert.assertTrue(TestUtil.CredentialStoreTaskTestInjector.INSTANCE.getNames().contains(pipelineId + "/" + secretName));
      CredentialValue value = TestUtil.CredentialStoreTaskTestInjector.INSTANCE.get(
          "",
          pipelineId + "/" + secretName,
          ""
      );
      Assert.assertEquals(secretValue, value.get());
    } finally {
      Files.delete(tempFile);
    }
  }


  @Test
  public void deleteSecret() throws Exception {
    String pipelineId = "PIPELINE_VAULT_pipelineId";
    String secretName = "stage_id_config1";
    String secretValue = "secretValue";
    TestUtil.CredentialStoreTaskTestInjector.INSTANCE.store(CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,pipelineId + "/" + secretName, secretValue);

    String secretPath = pipelineId + "/" + secretName;

    Response response = target(
        Utils.format(
            "/v1/secrets/{}/ctx=SecretManage",
            URLEncoder.encode(secretPath, StandardCharsets.UTF_8.name())
        )
    ).request().delete();

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertFalse(TestUtil.CredentialStoreTaskTestInjector.INSTANCE.getNames().contains(pipelineId + "/" + secretName));
    Assert.assertEquals(0, TestUtil.CredentialStoreTaskTestInjector.INSTANCE.getNames().size());
  }

  @Test
  public void listSecrets() throws Exception {
    String pipelineId1 = "PIPELINE_VAULT_listSecrets1";
    String secret11 = "stage_id_config11";
    String secret12 = "stage_id_config12";

    String pipelineId2 = "PIPELINE_VAULT_listSecrets2";
    String secret21 = "stage_id_config21";
    String secret22 = "stage_id_config22";

    String secret = "nonPipelineSecret";

    TestUtil.CredentialStoreTaskTestInjector.INSTANCE.store(CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST, pipelineId1 + "/" + secret11, "secret11");
    TestUtil.CredentialStoreTaskTestInjector.INSTANCE.store(CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST, pipelineId1 + "/" + secret12, "secret11");

    TestUtil.CredentialStoreTaskTestInjector.INSTANCE.store(CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST, pipelineId2 + "/" + secret21, "secret21");
    TestUtil.CredentialStoreTaskTestInjector.INSTANCE.store(CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST, pipelineId2 + "/" + secret22, "secret21");

    TestUtil.CredentialStoreTaskTestInjector.INSTANCE.store(CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST, secret, "secret");

    Response response = target("/v1/secrets/ctx=SecretList")
        .queryParam(PaginationInfoInjectorBinder.ORDER_BY_PARAM, "")
        .queryParam(PaginationInfoInjectorBinder.OFFSET_PARAM, "0")
        .queryParam(PaginationInfoInjectorBinder.LEN_PARAM, "-1")
        .request()
        .get();

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    InputStream responseEntity = (InputStream) response.getEntity();

    OkPaginationRestResponse<RSecret> secrets = ObjectMapperFactory.get().readValue(
        responseEntity,
        new TypeReference<OkPaginationRestResponse<RSecret>>() {}
    );

    List<RSecret> rSecrets = secrets.getData();

    Assert.assertEquals(5, rSecrets.size());
    Set<String> secretNames = rSecrets.stream().map(r -> {
      String s = r.getName().getValue();
      if (r.getVault().getValue() != null) {
        s = r.getVault().getValue() + "/" + s;
      }
      return s;
    }).collect(Collectors.toSet());
    Assert.assertTrue(
        secretNames.containsAll(
            ImmutableSet.of(
                pipelineId1 + "/" + secret11,
                pipelineId1 + "/" + secret12,
                pipelineId2 + "/" + secret21,
                pipelineId2 + "/" + secret22,
                secret
            )
        )
    );
  }

  @Test
  public void getSshTunnelPublicKey() throws Exception {
    String publicKeyVal = "publicKey";
    TestUtil.CredentialStoreTaskTestInjector.INSTANCE.store(
        CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,
        CredentialStoresTask.SSH_PUBLIC_KEY_SECRET,
        publicKeyVal
    );
    Response response = target("/v1/secrets/sshTunnelPublicKey").request().get();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    InputStream is = (InputStream)response.getEntity();
    Assert.assertEquals(publicKeyVal, IOUtils.toString(is));
  }

  @Test
  public void getSecretValueReady() {
    Response response = target("/v1/secrets/get").request().get();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }
}
