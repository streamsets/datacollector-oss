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
import com.google.common.base.Preconditions;
import com.streamsets.datacollector.restapi.rbean.rest.OkPaginationRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.OkRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.PaginationInfo;
import com.streamsets.datacollector.restapi.rbean.rest.RestRequest;
import com.streamsets.datacollector.util.AuthzRole;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.annotation.security.RolesAllowed;
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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Path("/v4/secrets")
@Produces(MediaType.APPLICATION_JSON)
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

  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("/text/ctx=SecretManage")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public OkRestResponse<RSecret> createSecret(RestRequest<RSecret> request) {
    RSecret rSecret = request.getData();
    Preconditions.checkArgument(rSecret.getType().getValue() == SecretType.TEXT,
        "Cannot create a secret of type: {} through this API", rSecret.getType().getValue());
    RSecret secret = null; //TODO create secret from text value
    return new  OkRestResponse<RSecret>().setHttpStatusCode(OkRestResponse.HTTP_CREATED).setData(secret);
  }

  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("/file/ctx=SecretManage")
  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public OkRestResponse<RSecret> createFileSecret(
      @FormDataParam("restRequest") String requestPayload,
      @FormDataParam("uploadedFile") InputStream upload
  ) throws IOException {
    RestRequest<RSecret> restRequest = RestRequest.getRequest(
        requestPayload,
        new TypeReference<RestRequest<RSecret>>() {}
    );
    RSecret rSecret = restRequest.getData();
    Preconditions.checkArgument(rSecret.getType().getValue() == SecretType.FILE,
        "Cannot upload a secret of type: {} through this API", rSecret.getType().getValue()
    );
    RSecret secret = null; //TODO create secret from upload stream
    return new  OkRestResponse<RSecret>().setHttpStatusCode(OkRestResponse.HTTP_CREATED).setData(secret);
  }

  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("{secretId}/text/ctx=SecretManage")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public OkRestResponse<RSecret> updateSecret(@PathParam("secretId") String id, RestRequest<RSecret> request) {
    RSecret rSecret = request.getData();
    Preconditions.checkArgument(rSecret.getType().getValue() == SecretType.TEXT,
        "Cannot create a secret of type: {} through this API", rSecret.getType().getValue());
    RSecret secret = null; //TODO update secret 'id' from text value
    return new  OkRestResponse<RSecret>().setHttpStatusCode(OkRestResponse.HTTP_CREATED).setData(secret);
  }

  @RolesAllowed({}) //TODO
  @Path("{secretId}/file/ctx=SecretManage")
  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public OkRestResponse<RSecret> updateFileSecret(
      @PathParam("secretId") String id,
      @FormDataParam("restRequest") String requestPayload,
      @FormDataParam("uploadedFile") InputStream upload
  ) throws IOException {
    RestRequest<RSecret> restRequest = RestRequest.getRequest(
        requestPayload,
        new TypeReference<RestRequest<RSecret>>() {}
    );
    RSecret rSecret = restRequest.getData();
    Preconditions.checkArgument(rSecret.getType().getValue() == SecretType.FILE,
        "Cannot upload a secret of type: {} through this API", rSecret.getType().getValue()
    );
    RSecret secret = null; //TODO update secret 'id' from upload stream
    return new  OkRestResponse<RSecret>().setHttpStatusCode(OkRestResponse.HTTP_CREATED).setData(secret);
  }

  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("/{secretId}/ctx=SecretManage")
  @DELETE
  @Consumes(MediaType.APPLICATION_JSON)
  public OkRestResponse<RSecret> deleteSecret(@PathParam("secretId") String id) {
    RSecret secret = null; //TODO delete secret
    return new OkRestResponse<RSecret>().setData(secret);
  }

  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("/pageId=SecretListPage")
  @GET
  public OkPaginationRestResponse<RSecret> listSecrets(@Context PaginationInfo paginationInfo) {
    List<RSecret> secrets = null; //TODO
    secrets.stream().forEach(s -> s.getValue().setScrubbed(true));
    return new OkPaginationRestResponse<RSecret>(paginationInfo).setData(secrets);
  }

  // Used by the S4 Cred Store to check if the Secrets App is up and ready.
  // The value is not used, only the 200 OK matters
  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("/get")
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Response getSecretValueReady() {
    // TODO if anything to do
    return Response.ok().build();
  }

  // Used by the S4 Cred Store to get a secret by name
  @RolesAllowed({AuthzRole.CREATOR, AuthzRole.ADMIN})
  @Path("/get/sdc/{secretId}/ctx=SecretGet")
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Response getSecretValue(@PathParam("secretId") String secretId) {
    RSecret secret = null; //TODO get secret
    return Response.ok(secret.getValue()).encoding("UTF-8").build();
  }



}
