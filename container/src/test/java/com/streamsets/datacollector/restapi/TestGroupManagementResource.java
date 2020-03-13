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

import com.streamsets.datacollector.restapi.rbean.lang.RString;
import com.streamsets.datacollector.restapi.rbean.rest.OkPaginationRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.OkRestResponse;
import com.streamsets.datacollector.restapi.rbean.rest.PaginationInfo;
import com.streamsets.datacollector.security.usermgnt.TrxUsersManager;
import com.streamsets.datacollector.security.usermgnt.UsersManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.UUID;

public class TestGroupManagementResource {
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
  public void testList() throws Exception {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);

    mgr.create("u1", "email", Arrays.asList("g1"),Arrays.asList("admin", "creator", "manager", "guest"));

    GroupManagementResource resource = new GroupManagementResource(mgr);

    OkPaginationRestResponse<RString> response = resource.list(new PaginationInfo());
    Assert.assertNotNull(response);
    Assert.assertEquals(OkRestResponse.HTTP_OK, response.getHttpStatusCode());
    Assert.assertEquals(Arrays.asList(new RString("all"), new RString("g1")), response.getData());
  }

}
