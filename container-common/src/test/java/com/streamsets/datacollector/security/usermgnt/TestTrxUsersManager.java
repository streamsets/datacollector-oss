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
package com.streamsets.datacollector.security.usermgnt;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.UUID;

public class TestTrxUsersManager {
  private File usersFile;

  @Before
  public void before() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    usersFile = new File(dir, "users.properties");
    try (Writer writer = new FileWriter(usersFile)) {
    }
  }

  @Test
  public void testCreateGet() throws IOException {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    Assert.assertEquals(0, mgr.listUsers().size());
    String resetPassword = mgr.create("u1", "e1", Arrays.asList("g1"), Arrays.asList("r1"));
    Assert.assertNotNull(resetPassword);
    User user = mgr.get("u1");
    Assert.assertEquals("u1", user.getUser());
    Assert.assertEquals("e1", user.getEmail());
    Assert.assertEquals(Arrays.asList("all", "g1"), user.getGroups());
    Assert.assertEquals(Arrays.asList("r1"), user.getRoles());
  }

  @Test
  public void testPasswordResetOk() throws IOException {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    Assert.assertEquals(0, mgr.listUsers().size());
    String resetPassword = mgr.create("u1", "e1", Arrays.asList("g1"), Arrays.asList("r1"));
    Assert.assertNotNull(resetPassword);
    resetPassword = mgr.resetPassword("u1");
    mgr.setPasswordFromReset("u1", resetPassword, "password");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPasswordResetExpired() throws Exception {
    UsersManager mgr = new TrxUsersManager(usersFile, 2);
    Assert.assertEquals(0, mgr.listUsers().size());
    String resetPassword = mgr.create("u1", "e1", Arrays.asList("g1"), Arrays.asList("r1"));
    try {
      Thread.sleep(3);
    } catch (InterruptedException ex) {
    }
    mgr.setPasswordFromReset("u1", resetPassword, "password");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPasswordResetInvalidReset() throws IOException {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    Assert.assertEquals(0, mgr.listUsers().size());
    String resetPassword = mgr.create("u1", "e1", Arrays.asList("g1"), Arrays.asList("r1"));
    mgr.resetPassword("u1");
    mgr.setPasswordFromReset("u1", resetPassword, "password");
  }

  @Test
  public void testChangePasswordOk() throws IOException {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    Assert.assertEquals(0, mgr.listUsers().size());
    String resetPassword = mgr.create("u1", "e1", Arrays.asList("g1"), Arrays.asList("r1"));
    Assert.assertNotNull(resetPassword);
    resetPassword = mgr.resetPassword("u1");
    mgr.setPasswordFromReset("u1", resetPassword, "password1");
    mgr.changePassword("u1", "password1", "password2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testChangePasswordFail() throws IOException {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    Assert.assertEquals(0, mgr.listUsers().size());
    String resetPassword = mgr.create("u1", "e1", Arrays.asList("g1"), Arrays.asList("r1"));
    Assert.assertNotNull(resetPassword);
    resetPassword = mgr.resetPassword("u1");
    mgr.setPasswordFromReset("u1", resetPassword, "password1");
    mgr.changePassword("u1", "password0", "password2");
  }

  @Test
  public void testDelete() throws IOException {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    Assert.assertEquals(0, mgr.listUsers().size());
    mgr.create("u1", "e1", Arrays.asList("g1"), Arrays.asList("r1"));
    Assert.assertEquals(1, mgr.listUsers().size());
    mgr.delete("u1");
    Assert.assertEquals(0, mgr.listUsers().size());
  }

  @Test
  public void testUpdate() throws IOException {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    Assert.assertEquals(0, mgr.listUsers().size());
    mgr.create("u1", "e1", Arrays.asList("g1"), Arrays.asList("r1"));
    mgr.update("u1", "e2", Arrays.asList("g2"), Arrays.asList("r2"));
    Assert.assertEquals(1, mgr.listUsers().size());
    Assert.assertEquals(Arrays.asList("r2"), mgr.listUsers().get(0).getRoles());
  }

  @Test
  public void testList() throws IOException {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    Assert.assertEquals(0, mgr.listUsers().size());
    mgr.create("u1", "e1", Arrays.asList("g1"), Arrays.asList("r1"));
    Assert.assertEquals(1, mgr.listUsers().size());
    Assert.assertEquals("u1", mgr.listUsers().get(0).getUser());
    Assert.assertEquals("e1", mgr.listUsers().get(0).getEmail());
    Assert.assertEquals(Arrays.asList("all", "g1"), mgr.listUsers().get(0).getGroups());
    Assert.assertEquals(Arrays.asList("r1"), mgr.listUsers().get(0).getRoles());
  }

  @Test
  public void testGroups() throws IOException {
    UsersManager mgr = new TrxUsersManager(usersFile, 10000);
    Assert.assertEquals(0, mgr.listUsers().size());
    mgr.create("u1", "e1", Arrays.asList("g2"), Arrays.asList("r1"));
    mgr.create("u2", "e2", Arrays.asList("g2", "g1"), Arrays.asList("r1"));
    Assert.assertEquals(Arrays.asList("all", "g1", "g2"), mgr.listGroups());
  }

}
