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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.restapi.RequiresCredentialsDeployed;
import com.streamsets.datacollector.restapi.rbean.lang.RDatetime;
import com.streamsets.datacollector.restapi.rbean.lang.RString;
import com.streamsets.datacollector.restapi.rbean.rest.OkPaginationRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.OkRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.PaginationInfo;
import com.streamsets.datacollector.restapi.rbean.rest.RestRequest;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.credential.ManagedCredentialStore;
import com.streamsets.pipeline.api.impl.Utils;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Path("/v4/secrets")
@Produces(MediaType.APPLICATION_JSON)
@RequiresCredentialsDeployed
public class SecretResource  {

  /**
   * Skeleton for Secrets API for SDC following designer pattern.
   *
   * NOTES:
   *       * PaginationInfoInjectorBinder must be registered with Jersey
   *       * make sure to register this resource with Jersey
   *       * the ObjectMapper used by SDC should be configured with RJson.configureRJson([ObjectManager])
   *       * there is no metadata
   *       * the NEW bean REST API is gone, caller must create bean from scratch
   *       * create uses POST instead of PUT
   *       * REST API URLs changed a bit
   *       * Assuming there is a single vault, 'sdc'
   *       * There are no ACLs on secrets
   */

  public static final String SSH_PUBLIC_KEY_SECRET = "sdc/defaultPublicKey";

  private ManagedCredentialStore managedCredentialStore;

  @Inject
  public SecretResource(CredentialStoresTask task) {
    managedCredentialStore = task.getDefaultManagedCredentialStore();
  }

  private void checkCredentialStoreSupported() {
    Preconditions.checkNotNull(managedCredentialStore, "Managed Credential store not configured");
  }

  private void setVaultAndSecretNameFromCredentialName(RSecret rSecret, String credentialName) {
    String[] splitByVaultAndSecretName = credentialName.split("/");
    if (splitByVaultAndSecretName.length == 2) {
      String vaultName = splitByVaultAndSecretName[0];
      String secretName = splitByVaultAndSecretName[1];
      rSecret.setVault(new RString().setValue(vaultName));
      rSecret.setName(new RString().setValue(secretName));
    } else {
      rSecret.setName(new RString().setValue(credentialName));
    }
  }

  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("/text/ctx=SecretManage")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public OkRestResponse<RSecret> createSecret(RestRequest<RSecret> request) {
    RSecret rSecret = request.getData();
    checkCredentialStoreSupported();
    Preconditions.checkArgument(rSecret.getType().getValue() == SecretType.TEXT,
        "Cannot create a secret of type: {} through this API",
        rSecret.getType().getValue()
    );

    managedCredentialStore.store(
        CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,
        rSecret.getVault().getValue() + "/" + rSecret.getName().getValue(),
        rSecret.getValue().getValue()
    );

    rSecret.getValue().setScrubbed(true);
    rSecret.setCreatedOn(new RDatetime(System.currentTimeMillis()));
    rSecret.setLastModifiedOn(new RDatetime(System.currentTimeMillis()));
    return new OkRestResponse<RSecret>().setHttpStatusCode(OkRestResponse.HTTP_CREATED).setData(rSecret);
  }

//  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
//  @Path("/file/ctx=SecretManage")
//  @POST
//  @Consumes(MediaType.MULTIPART_FORM_DATA)
//  @Produces(MediaType.APPLICATION_JSON)
//  public OkRestResponse<RSecret> createFileSecret(
//      @FormDataParam("restRequest") String requestPayload,
//      @FormDataParam("uploadedFile") InputStream upload
//  ) throws IOException {
//    RestRequest<RSecret> restRequest = RestRequest.getRequest(
//        requestPayload,
//        new TypeReference<RestRequest<RSecret>>() {}
//    );
//    RSecret rSecret = restRequest.getData();
//    Preconditions.checkArgument(rSecret.getType().getValue() == SecretType.FILE,
//        "Cannot upload a secret of type: {} through this API", rSecret.getType().getValue()
//    );
//
//    return new OkRestResponse<RSecret>().setHttpStatusCode(OkRestResponse.HTTP_CREATED).setData(rSecret);
//  }

  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("{secretId}/text/ctx=SecretManage")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public OkRestResponse<RSecret> updateSecret(@PathParam("secretId") String id, RestRequest<RSecret> request) {
    RSecret rSecret = request.getData();
    checkCredentialStoreSupported();
    Preconditions.checkArgument(rSecret.getType().getValue() == SecretType.TEXT,
        "Cannot create a secret of type: {} through this API", rSecret.getType().getValue());
    managedCredentialStore.store(
        CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,
        rSecret.getVault().getValue() + "/" + rSecret.getName().getValue(),
        rSecret.getValue().getValue()
    );
    rSecret.setLastModifiedOn(new RDatetime());
    rSecret.getValue().setScrubbed(true);

    return new  OkRestResponse<RSecret>().setHttpStatusCode(OkRestResponse.HTTP_CREATED).setData(rSecret);
  }

  //  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  //  @Path("{secretId}/file/ctx=SecretManage")
  //  @POST
  //  @Consumes(MediaType.MULTIPART_FORM_DATA)
  //  @Produces(MediaType.APPLICATION_JSON)
  //  public OkRestResponse<RSecret> updateFileSecret(
  //      @PathParam("secretId") String id,
  //      @FormDataParam("restRequest") String requestPayload,
  //      @FormDataParam("uploadedFile") InputStream upload
  //  ) throws IOException {
  //    Preconditions.checkNotNull(managedCredentialStore, "Managed Credential store not configured");
  //    RestRequest<RSecret> restRequest = RestRequest.getRequest(
  //        requestPayload,
  //        new TypeReference<RestRequest<RSecret>>() {}
  //    );
  //    RSecret rSecret = restRequest.getData();
  //    Preconditions.checkArgument(rSecret.getType().getValue() == SecretType.FILE,
  //        "Cannot upload a secret of type: {} through this API", rSecret.getType().getValue()
  //    );
  //    managedCredentialStore.
  //    rSecret.setCreatedOn(new RDatetime());
  //    rSecret.setLastModifiedOn(new RDatetime());
  //    return new OkRestResponse<RSecret>().setHttpStatusCode(OkRestResponse.HTTP_CREATED).setData(rSecret);
  //  }

  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("/{secretId}/ctx=SecretManage")
  @DELETE
  @Consumes(MediaType.APPLICATION_JSON)
  public OkRestResponse<RSecret> deleteSecret(@PathParam("secretId") String id) {
    checkCredentialStoreSupported();
    managedCredentialStore.delete(id);
    RSecret rSecret = new RSecret();
    setVaultAndSecretNameFromCredentialName(rSecret, id);
    return new OkRestResponse<RSecret>().setData(rSecret);
  }

  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("/ctx=SecretList")
  @GET
  public OkPaginationRestResponse<RSecret> listSecrets(@Context PaginationInfo paginationInfo) {
    checkCredentialStoreSupported();
    List<RSecret> secrets = managedCredentialStore.getNames().stream().map(
        s -> {
          RSecret rSecret = new RSecret();
          setVaultAndSecretNameFromCredentialName(rSecret, s);
          rSecret.getValue().setScrubbed(true);
          return rSecret;
        }
    ).collect(Collectors.toList());
    return new OkPaginationRestResponse<RSecret>(paginationInfo).setData(secrets);
  }

  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("/sshTunnelPublicKey")
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Response getSshTunnelPublicKey() {
    checkCredentialStoreSupported();
    CredentialValue publicKeyVal = managedCredentialStore.get(CredentialStoresTask.DEFAULT_SDC_GROUP, SSH_PUBLIC_KEY_SECRET, null);
    if (publicKeyVal != null) {
      String publicKey = publicKeyVal.get();
      if (!Strings.isNullOrEmpty(publicKey)) {
        return Response.ok(publicKey).build();
      } else {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Data Collector's public key is empty").build();
      }
    } else {
      return Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(
              Utils.format(
                  "Data Collector's managed credential store {} is not seeded with public key," +
                      " please check the configuration"
              )
          ).build();
    }
  }

  // Used by the S4 Cred Store to check if the Secrets App is up and ready.
  // The value is not used, only the 200 OK matters
  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("/get")
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Response getSecretValueReady() {
    if (managedCredentialStore == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    return Response.ok().build();
  }

}
