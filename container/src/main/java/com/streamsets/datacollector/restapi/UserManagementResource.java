/**
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.restapi;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.publicrestapi.usermgnt.RSetPassword;
import com.streamsets.datacollector.restapi.rbean.lang.RString;
import com.streamsets.datacollector.restapi.rbean.rest.OkPaginationRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.OkRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.PaginationInfo;
import com.streamsets.datacollector.restapi.rbean.rest.RestRequest;
import com.streamsets.datacollector.restapi.rbean.rest.RestResource;
import com.streamsets.datacollector.restapi.rbean.usermgnt.RChangePassword;
import com.streamsets.datacollector.restapi.rbean.usermgnt.RResetPasswordLink;
import com.streamsets.datacollector.restapi.rbean.usermgnt.RUser;
import com.streamsets.datacollector.security.usermgnt.User;
import com.streamsets.datacollector.security.usermgnt.UsersManager;
import com.streamsets.datacollector.util.AuthzRole;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import org.jetbrains.annotations.NotNull;

import javax.annotation.security.PermitAll;
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
import java.io.IOException;
import java.security.Principal;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

@Path("/v1/usermanagement/users")
@RolesAllowed(AuthzRole.ADMIN)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "usermanagement")
public class UserManagementResource extends RestResource {
  private final Principal principal;
  private final UsersManager usersManager;

  @Inject
  public UserManagementResource(UsersManager usersManager, Principal principal) {
    this.principal = principal;
    this.usersManager = usersManager;
  }


  private RUser getUser(RestRequest<RUser> request) {
    Preconditions.checkArgument(request != null, "Missing payload");
    RUser user = request.getData();
    Preconditions.checkArgument(user != null, "Missing user");
    Preconditions.checkArgument(user.getId() != null, "Missing user name");
    Preconditions.checkArgument(user.getRoles() != null, "Missing user roles");
    return user;
  }

  @NotNull
  RResetPasswordLink createResetPasswordLink(@PathParam("id") String id, boolean hasEmail, String resetToken)
      throws IOException {
    RSetPassword setPassword = new RSetPassword();
    setPassword.getId().setValue("");
    setPassword.getResetToken().setValue(resetToken);
    setPassword.getPassword().setValue("");
    RestRequest<RSetPassword> request = new RestRequest<>();
    request.setData(setPassword);
    String requestJson = ObjectMapperFactory.getOneLine().writeValueAsString(request);
    String requestJsonB64 = Base64.getUrlEncoder().encodeToString(requestJson.getBytes());

    //TODO create LINK with token
    String link = "http://localhost:18630/resetPassword?token=" + requestJsonB64;

    //TODO determine if SMTP server is configured
    boolean smtpServerConfigured = false;

    RResetPasswordLink resetLink = new RResetPasswordLink();
    if (hasEmail && smtpServerConfigured) {
      User user = usersManager.get(id);
      if (user != null) {
        String email = null; //TODO
        if (email != null) {
          resetLink.getSentByEmail().setValue(true);
          //TODO sent by email.
        } else {
          //TODO ERROR
        }
      } else {
        //TODO ERROR
      }
    } else {
      resetLink.getLink().setValue(link);
      resetLink.getSentByEmail().setValue(false);
    }
    return resetLink;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Creates User", authorizations = @Authorization(value = "basic"))
  public OkRestResponse<RResetPasswordLink> create(RestRequest<RUser> request) throws IOException {
    RUser user = getUser(request);
    String resetToken = usersManager.create(
        user.getId().getValue(),
        user.getEmail().getValue(),
        user.getGroups().stream().map(g -> g.getValue()).collect(Collectors.toList()),
        user.getRoles().stream().map(r -> r.getValue()).collect(Collectors.toList())
    );
    Preconditions.checkNotNull(resetToken, "User already exists");
    RResetPasswordLink resetLink = createResetPasswordLink(
        user.getId().getValue(),
        !user.getEmail().getValue().isEmpty(),
        resetToken
    );
    return new OkRestResponse<RResetPasswordLink>().setHttpStatusCode(OkRestResponse.HTTP_CREATED).setData(resetLink);
  }

  @Path("/{id}")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update User", authorizations = @Authorization(value = "basic"))
  public OkRestResponse<RUser> update(@PathParam("id") String id, RestRequest<RUser> request) throws IOException {
    RUser user = getUser(request);
    Preconditions.checkArgument(id.equals(user.getId().getValue()), "Payload user ID does not match ID in URL");
    List<String> groups = user.getGroups()
        .stream()
        .map(r -> r.getValue())
        .collect(Collectors.toList());
    List<String> roles = user.getRoles()
        .stream()
        .map(r -> r.getValue())
        .collect(Collectors.toList());
    usersManager.update(user.getId().getValue(), user.getEmail().getValue(), groups, roles);
    return new OkRestResponse<RUser>().setHttpStatusCode(OkRestResponse.HTTP_OK).setData(user);
  }

  @Path("/{id}")
  @DELETE
  @ApiOperation(value = "Delete User", authorizations = @Authorization(value = "basic"))
  public OkRestResponse<Void> delete(@PathParam("id") String id) throws IOException {
    usersManager.delete(id);
    return new OkRestResponse<Void>().setHttpStatusCode(OkRestResponse.HTTP_OK);
  }

  private boolean isValidRole(String role) {
    for (String r : AuthzRole.ALL_ROLES) {
      if (r.equals(role)) {
        return true;
      }
    }
    return false;
  }

  @GET
  @ApiOperation(value = "List Users", authorizations = @Authorization(value = "basic"))
  public OkPaginationRestResponse<RUser> list(@Context PaginationInfo paginationInfo) throws IOException {
    paginationInfo = (paginationInfo != null) ? paginationInfo : new PaginationInfo();
    List<RUser> users = usersManager.listUsers().stream().map(u -> {
      RUser user = new RUser();
      user.getId().setValue(u.getUser());
      user.getEmail().setValue(u.getEmail());
      List<RString> rGroups = u.getGroups()
          .stream()
          .map(r -> new RString(r))
          .collect(Collectors.toList());
      user.setGroups(rGroups);
      List<RString> rRoles = u.getRoles()
          .stream()
          .filter(r -> isValidRole(r))
          .map(r -> new RString(r))
          .collect(Collectors.toList());
      user.setRoles(rRoles);
      return user;
    }).collect(Collectors.toList());

    return new OkPaginationRestResponse<RUser>(paginationInfo).setHttpStatusCode(OkRestResponse.HTTP_OK).setData(users);
  }

  @Path("/{id}/changePassword")
  @PermitAll
  @POST
  @ApiOperation(value = "Change Password for User", authorizations = @Authorization(value = "basic"))
  public OkRestResponse<Void> changePassword(@PathParam("id") String id, RestRequest<RChangePassword> request)
      throws IOException {
    Preconditions.checkArgument(request != null, "Missing payload");
    RChangePassword changePassword = request.getData();
    Preconditions.checkArgument(changePassword != null, "Missing changePassword");
    Preconditions.checkArgument(
        principal.getName().equals(changePassword.getId().getValue()),
        "Payload user ID does not match current user ID"
    );
    Preconditions.checkArgument(
        principal.getName().equals(id),
        "User ID in URL does not match current user ID"
    );
    usersManager.changePassword(
              changePassword.getId().getValue(),
              changePassword.getOldPassword().getValue(),
              changePassword.getNewPassword().getValue()
    );
    return new OkRestResponse<Void>().setHttpStatusCode(OkRestResponse.HTTP_NO_CONTENT);
  }


  @Path("/{id}/resetPassword")
  @POST
  @ApiOperation(value = "Reset Password for User", authorizations = @Authorization(value = "basic"))
  public OkRestResponse<RResetPasswordLink> resetPassword(@PathParam("id") String id) throws IOException {
    User user = usersManager.get(id);
    Preconditions.checkNotNull(user, "user not found");
    String resetToken = usersManager.resetPassword(id);
    RResetPasswordLink resetLink = createResetPasswordLink(id, !user.getEmail().isEmpty(), resetToken);
    return new OkRestResponse<RResetPasswordLink>().setHttpStatusCode(OkRestResponse.HTTP_OK).setData(resetLink);
  }

}
