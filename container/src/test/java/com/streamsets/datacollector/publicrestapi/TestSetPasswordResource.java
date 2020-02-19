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

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.publicrestapi.usermgnt.RSetPassword;
import com.streamsets.datacollector.restapi.rbean.rest.OkRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.RestRequest;
import com.streamsets.datacollector.security.usermgnt.UserManagementExecutor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.UUID;

public class TestSetPasswordResource {
  private RuntimeInfo runtimeInfo;
  private File usersFile;

  @Before
  public void before() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getConfigDir()).thenReturn(dir.getAbsolutePath());

    usersFile = new File(dir, "/form-realm.properties");
    try (Writer writer = new FileWriter(usersFile)) {
    }
  }

  @Test
  public void testSetPassword() throws Exception {
    UserManagementExecutor executor = new UserManagementExecutor(usersFile, 10000);

    String resetToken = executor.execute(mgr -> mgr.create(
        "u1",
        "email",
        Arrays.asList("g1"),
        Arrays.asList("creator")
    ));

    SetPasswordResource resource = new SetPasswordResource(runtimeInfo);
    RSetPassword setPassword = new RSetPassword();
    setPassword.getId().setValue("u1");
    setPassword.getResetToken().setValue(resetToken);
    setPassword.getPassword().setValue("pass");
    RestRequest<RSetPassword> request = new RestRequest<>();
    request.setData(setPassword);
    OkRestResponse<Void> response = resource.setPassword(request);
    Assert.assertEquals(OkRestResponse.HTTP_OK, response.getHttpStatusCode());

    executor.execute(mgr -> {
      Assert.assertTrue(mgr.verifyPassword("u1", "pass"));
      return null;
    });
  }
}
