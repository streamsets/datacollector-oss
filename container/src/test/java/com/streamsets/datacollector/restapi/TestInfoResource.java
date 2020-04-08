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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.util.AuthzRole;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.util.List;
import java.util.Map;

public class TestInfoResource {

  @Test
  public void testCurrentUser() throws Exception {
    UserGroupManager userGroupManager = Mockito.mock(UserGroupManager.class);
    InfoResource resource = new InfoResource(null, null, null, userGroupManager);

    Response response1 = resource.getUserInfo(createSecurityContext(AuthzRole.ADMIN));
    assertUserRole(response1, AuthzRole.ADMIN);

    // remote role
    Response response2 = resource.getUserInfo(createSecurityContext(AuthzRole.GUEST_REMOTE));
    assertUserRole(response2, AuthzRole.GUEST);

    // multiple roles
    Response response3 = resource.getUserInfo(createSecurityContext(AuthzRole.MANAGER, AuthzRole.CREATOR_REMOTE));
    assertUserRole(response3, AuthzRole.MANAGER);
    assertUserRole(response3, AuthzRole.CREATOR);

    Response response4 = resource.getUserInfo(createSecurityContext(AuthzRole.ADMIN_ACTIVATION));
    assertUserRole(response4, AuthzRole.ADMIN_ACTIVATION);
  }

  /**
   * @param userRoles the list of user roles
   * @return the mock SecurityContext with the given user roles
   */
  private static SecurityContext createSecurityContext(String... userRoles) {
    SecurityContext securityContext = Mockito.mock(SecurityContext.class);
    Principal principal = Mockito.mock(Principal.class);
    Mockito.when(securityContext.getUserPrincipal()).thenReturn(principal);
    Mockito.when(principal.getName()).thenReturn("user");
    for (String role : userRoles) {
      Mockito.when(securityContext.isUserInRole(role)).thenReturn(true);
    }
    return securityContext;
  }

  /**
   * Asserts that the response has the given role.
   */
  private static void assertUserRole(Response response, String role) {
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Map<String, Object> entity = (Map<String, Object>) response.getEntity();
    Assert.assertTrue(((List<String>)entity.get("roles")).contains(role));
  }
}
