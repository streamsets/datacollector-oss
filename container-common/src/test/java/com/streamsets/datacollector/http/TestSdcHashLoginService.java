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
package com.streamsets.datacollector.http;

import org.eclipse.jetty.security.AbstractLoginService.UserPrincipal;
import org.eclipse.jetty.security.PropertyUserStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;

public class TestSdcHashLoginService {

  @Test
  public void testRoleInfo() {
    SdcHashLoginService loginService = new SdcHashLoginService();
    loginService.setUserStore(new TestPropertyUserStore(
        "admin:   MD5:21232f297a57a5a743894a0e4a801fc3,user,email:,admin,group:streamsets"
    ));

    UserPrincipal userPrincipal = new UserPrincipal("admin", null);
    String[] roles = loginService.loadRoleInfo(userPrincipal);
    Arrays.sort(roles);
    Assert.assertEquals(2, roles.length);
    Assert.assertArrayEquals(new String[]{"admin", "user"}, roles);
  }

  /**
   * PropertyUserStore that takes lines and load users using a temporary file.
   */
  private static class TestPropertyUserStore extends PropertyUserStore {
    public TestPropertyUserStore(String userLine) {
      try {
        File realmFile = File.createTempFile("test", ".properties");
        try (FileWriter writer = new FileWriter(realmFile)) {
          writer.write(userLine);
        }
        realmFile.deleteOnExit();
        setConfigFile(realmFile);
        loadUsers();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
