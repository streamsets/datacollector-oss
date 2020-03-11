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
package com.streamsets.datacollector.publicrestapi;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.publicrestapi.usermgnt.RSetPassword;
import com.streamsets.datacollector.restapi.rbean.rest.OkRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.RestRequest;
import com.streamsets.datacollector.restapi.rbean.rest.RestResource;
import com.streamsets.datacollector.security.usermgnt.UserManagementExecutor;
import com.streamsets.datacollector.util.AuthzRole;

import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;

@Path("/v1/usermanagement/users")
@RolesAllowed(AuthzRole.ADMIN)
@Produces(MediaType.APPLICATION_JSON)
public class SetPasswordResource extends RestResource {
  private final UserManagementExecutor executor;

  @Inject
  public SetPasswordResource(RuntimeInfo runtimeInfo) {
    File usersFile = new File(runtimeInfo.getConfigDir(), "/form-realm.properties");
    executor = new UserManagementExecutor(usersFile, 0); // resetValidity not used here
  }

  @Path("/setPassword")
  @PermitAll
  @POST
  public OkRestResponse<Void> setPassword(RestRequest<RSetPassword> request) throws IOException {
    Preconditions.checkArgument(request != null, "Missing payload");
    RSetPassword setPassword = request.getData();
    Preconditions.checkArgument(setPassword != null, "Missing setPassword");
    executor.execute(mgr -> {
          mgr.setPasswordFromReset(
              setPassword.getId().getValue(),
              setPassword.getResetToken().getValue(),
              setPassword.getPassword().getValue()
          );
          return null;
        }
    );
    return new OkRestResponse<Void>().setHttpStatusCode(OkRestResponse.HTTP_NO_CONTENT);
  }

}
