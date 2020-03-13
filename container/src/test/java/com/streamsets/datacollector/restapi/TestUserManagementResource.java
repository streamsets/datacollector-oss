/**
 * Copyright 2020 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.restapi;

import com.fasterxml.jackson.core.type.TypeReference;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.publicrestapi.usermgnt.RSetPassword;
import com.streamsets.datacollector.restapi.rbean.lang.RString;
import com.streamsets.datacollector.restapi.rbean.rest.OkPaginationRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.OkRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.PaginationInfo;
import com.streamsets.datacollector.restapi.rbean.rest.RestRequest;
import com.streamsets.datacollector.restapi.rbean.usermgnt.RChangePassword;
import com.streamsets.datacollector.restapi.rbean.usermgnt.RResetPasswordLink;
import com.streamsets.datacollector.restapi.rbean.usermgnt.RUser;
import com.streamsets.datacollector.security.usermgnt.TrxUsersManager;
import com.streamsets.datacollector.security.usermgnt.UsersManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

public class TestUserManagementResource {
  private File usersFile;

  @Before
  public void before() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());

    usersFile = new File(dir, "/form-realm.properties");
    try (Writer writer = new FileWriter(usersFile)) {
    }
  }

  @Test
  public void testCreate() throws Exception {
    UsersManager mgr = new TrxUsersManager(usersFile);

    UserManagementResource resource = new UserManagementResource(mgr, null);

    RUser user = new RUser();
    user.getId().setValue("u1");
    user.getEmail().setValue("e1");
    user.setGroups(Arrays.asList(new RString("g1")));
    user.setRoles(Arrays.asList(new RString("admin")));
    RestRequest<RUser> request = new RestRequest<>();
    request.setData(user);

    OkRestResponse<RResetPasswordLink> response = resource.create(request);
    Assert.assertNotNull(response);
    Assert.assertEquals(OkRestResponse.HTTP_CREATED, response.getHttpStatusCode());

    mgr = new TrxUsersManager(usersFile, 10000);

    Assert.assertEquals(1, mgr.listUsers().size());
    Assert.assertEquals("u1", mgr.listUsers().get(0).getUser());
    Assert.assertEquals("e1", mgr.listUsers().get(0).getEmail());
    Assert.assertEquals(Arrays.asList("all", "g1"), mgr.listUsers().get(0).getGroups());
    Assert.assertEquals(Arrays.asList("admin"), mgr.listUsers().get(0).getRoles());
  }

  @Test
  public void testUpdate() throws Exception {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    mgr.create("u1", "email", Arrays.asList("g1"), Arrays.asList("admin"));

    UserManagementResource resource = new UserManagementResource(mgr, null);

    RUser user = new RUser();
    user.getId().setValue("u1");
    user.getEmail().setValue("e2");
    user.setGroups(Arrays.asList(new RString("g2")));
    user.setRoles(Arrays.asList(new RString("creator")));
    RestRequest<RUser> request = new RestRequest<>();
    request.setData(user);

    OkRestResponse<RUser> response = resource.update("u1", request);
    Assert.assertNotNull(response);
    Assert.assertEquals(OkRestResponse.HTTP_OK, response.getHttpStatusCode());

    Assert.assertEquals("u1", response.getData().getId().getValue());
    Assert.assertEquals(Arrays.asList(new RString("creator")), response.getData().getRoles());

    Assert.assertEquals(1, mgr.listUsers().size());
    Assert.assertEquals("u1", mgr.listUsers().get(0).getUser());
    Assert.assertEquals("e2", mgr.listUsers().get(0).getEmail());
    Assert.assertEquals(Arrays.asList("all", "g2"), mgr.listUsers().get(0).getGroups());
    Assert.assertEquals(Arrays.asList("creator"), mgr.listUsers().get(0).getRoles());
  }

  @Test
  public void testDelete() throws Exception {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    mgr.create("u1", "email", Arrays.asList("g1"), Arrays.asList("admin"));

    UserManagementResource resource = new UserManagementResource(mgr, null);

    OkRestResponse<Void> response = resource.delete("u1");
    Assert.assertNotNull(response);
    Assert.assertEquals(OkRestResponse.HTTP_OK, response.getHttpStatusCode());

    Assert.assertEquals(0, mgr.listUsers().size());
  }

  @Test
  public void testList() throws Exception {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    mgr.create("u1", "email", Arrays.asList("g1"),Arrays.asList("admin", "creator", "manager", "guest"));

    UserManagementResource resource = new UserManagementResource(mgr, null);

    OkPaginationRestResponse<RUser> response = resource.list(new PaginationInfo());
    Assert.assertNotNull(response);
    Assert.assertEquals(OkRestResponse.HTTP_OK, response.getHttpStatusCode());

    List<RUser> users = response.getData();
    Assert.assertEquals(1, users.size());
    RUser user = users.get(0);
    Assert.assertEquals("u1", user.getId().getValue());
    Assert.assertEquals("email", user.getEmail().getValue());
    Assert.assertEquals(Arrays.asList(new RString("all"), new RString("g1")), user.getGroups());
    Assert.assertEquals(Arrays.asList(new RString("admin"),
        new RString("creator"),
        new RString("manager"),
        new RString("guest")
    ), user.getRoles());
  }

  @Test
  public void testChangePassword() throws Exception {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);

    String resetToken = mgr.create("u1", "email", Arrays.asList("g1"), Arrays.asList("admin"));
    mgr.setPasswordFromReset("u1", resetToken, "password");

    UserManagementResource resource = new UserManagementResource(mgr, () -> "u1");

    RChangePassword changePassword = new RChangePassword();
    changePassword.getId().setValue("u1");
    changePassword.getOldPassword().setValue("password");
    changePassword.getNewPassword().setValue("PASSWORD");

    RestRequest<RChangePassword> request = new RestRequest<>();
    request.setData(changePassword);

    OkRestResponse<Void> response = resource.changePassword("u1", request);
    Assert.assertNotNull(response);
    Assert.assertEquals(OkRestResponse.HTTP_NO_CONTENT, response.getHttpStatusCode());

    mgr.verifyPassword("u1", "PASSWORD");
  }

  @Test
  public void testResetPassword() throws Exception {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    String resetToken = mgr.create("u1", "email", Arrays.asList("g1"), Arrays.asList("admin"));
    mgr.setPasswordFromReset("u1", resetToken, "password");

    UserManagementResource resource = new UserManagementResource(mgr, () -> "u1");

    OkRestResponse<RResetPasswordLink> response = resource.resetPassword("u1");
    Assert.assertNotNull(response);
    Assert.assertEquals(OkRestResponse.HTTP_OK, response.getHttpStatusCode());
    String link = response.getData().getLink().getValue();
    Assert.assertNotNull(link);
    int anchorIdx = link.indexOf("?token=");
    Assert.assertTrue(anchorIdx > -1);
    String b64 = link.substring(anchorIdx + "?token=".length());
    String json = new String(Base64.getDecoder().decode(b64));
    RestRequest<RSetPassword> request = ObjectMapperFactory.getOneLine().readValue(
        json,
        new TypeReference<RestRequest<RSetPassword>>() {
        }
    );
    Assert.assertNotNull(request);
    RSetPassword setPassword = request.getData();
    Assert.assertNotNull(setPassword);
    Assert.assertEquals("", setPassword.getId().getValue());
    Assert.assertEquals("", setPassword.getPassword().getValue());
    Assert.assertNotNull(setPassword.getResetToken().getValue());

    mgr.setPasswordFromReset("u1", setPassword.getResetToken().getValue(), "PASSWORD");
  }


}
