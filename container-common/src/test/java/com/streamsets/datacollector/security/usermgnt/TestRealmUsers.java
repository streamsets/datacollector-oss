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
package com.streamsets.datacollector.security.usermgnt;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;

public class TestRealmUsers {

  private final static String REALM_DATA =
      "#\n" +
      "\n" +
      "# FORM authentication, password is same as user name\n" +
      "admin: MD5:21232f297a57a5a743894a0e4a801fc3,user,email:,admin\n" +
      "guest: MD5:084e0343a0486ff05530df6c705c8bb4,user,email:,guest\n" +
      "creator: MD5:ee2433259b0fe399b40e81d2c98a38b6,user,email:,group:foo,creator\n" +
      "manager: MD5:1d0258c2440a8d19e716292b231e3190,user,email:manager@acme,manager\n" +
      "escaped: MD5\\:1d0258c2440a8d19e716292b231e3190,user,email\\:escaped@example.com,guest\n" +
      "\n" +
      "#\n";

  @Test
  public void testParse() throws IOException  {
    RealmUsers realmUsers = RealmUsers.parse(new BufferedReader(new StringReader(REALM_DATA)));
    Assert.assertEquals(5, realmUsers.list().size());
    Assert.assertNotNull(realmUsers.find("admin"));
    Assert.assertNotNull(realmUsers.find("guest"));
    Assert.assertNotNull(realmUsers.find("creator"));
    Assert.assertNotNull(realmUsers.find("manager"));
    Assert.assertNotNull(realmUsers.find("escaped"));
    Assert.assertEquals(Arrays.asList("manager"), realmUsers.find("manager").getRoles());
    Assert.assertEquals("escaped@example.com", realmUsers.find("escaped").getEmail());
  }

  @Test
  public void testWrite() throws IOException  {
    RealmUsers realmUsers = RealmUsers.parse(new BufferedReader(new StringReader(REALM_DATA)));
    StringWriter writer = new StringWriter();
    realmUsers.write(writer);
    writer.close();
    Assert.assertEquals(RealmUsers.ESCAPED_PATTERN.matcher(REALM_DATA).replaceAll("$1"), writer.toString());
  }

  @Test
  public void testAddDelete() throws IOException  {
    RealmUsers realmUsers = RealmUsers.parse(new BufferedReader(new StringReader("")));
    Assert.assertEquals(0, realmUsers.list().size());
    UserLine ul = UserLineCreator.getMD5Creator().create("user", "email", Arrays.asList("g1", "g2"), Arrays.asList("r1", "r2"), "password");
    realmUsers.add(ul);
    Assert.assertEquals(1, realmUsers.list().size());
    realmUsers.delete("user");
    Assert.assertEquals(0, realmUsers.list().size());
    Assert.assertNull(realmUsers.find("user"));
  }

}
