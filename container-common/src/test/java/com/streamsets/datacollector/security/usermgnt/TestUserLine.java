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

import com.streamsets.pipeline.lib.parser.shaded.com.google.code.regexp.Matcher;
import com.streamsets.pipeline.lib.parser.shaded.com.google.code.regexp.Pattern;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestUserLine {

  @Test
  public void testUserLineRegex() {
    String regex = UserLine.getUserLineRegex("XYZ");

    Pattern pattern = Pattern.compile(regex);

    Assert.assertTrue(pattern.matcher("user@foo.com: XYZ:abcd-:wr0233240x,user,a,b,c,d").matches());
    Assert.assertTrue(pattern.matcher("user@foo.com:  XYZ:abcd-:wr0233240x,user,").matches());
    Assert.assertTrue(pattern.matcher("user: XYZ:abcd-:wr0233240x,user,a,b,c,d").matches());
    Assert.assertTrue(pattern.matcher(" user@foo.com: XYZ:abcd,user,a,b,c,d").matches());
    Assert.assertTrue(pattern.matcher(" user@foo.com: XYZ:abcd,user").matches());
    Assert.assertFalse(pattern.matcher("user @foo.com: XYZ:abcd-:wr0233240x,user,a,b,c,d").matches());
    Assert.assertFalse(pattern.matcher("user@foo.com: ABC:abcd-:wr0233240x,user,a,b,c,d").matches());
    Assert.assertFalse(pattern.matcher("user @foo.com: XYZ: abcd-:wr0233240x,user,a,b,c,d").matches());
    Assert.assertFalse(pattern.matcher("user @foo.com: XYZ:abcd-:wr0233240x,userx,a,b,c,d").matches());
    Assert.assertFalse(pattern.matcher("user @foo.com: XYZ:abcd-:wr0233240x,user,a,b,c,d").matches());

    Matcher matcher = pattern.matcher("user@foo.com: XYZ:abcd-:wr0233240x,user,a,b,c,d");
    Assert.assertTrue(matcher.matches());
    Assert.assertEquals("user@foo.com", matcher.group(1));
    Assert.assertEquals("abcd-:wr0233240x", matcher.group(2));
    Assert.assertEquals(",a,b,c,d", matcher.group(3));
  }

  // using MD5UserLine subclass to test the UserLine functionality

  @Test
  public void testNewUserLine() {
    UserLine ul = new MD5UserLine("USER", "EMAIL", Arrays.asList("g1", "g2"), Arrays.asList("r1", "r2"), "PASSWORD");
    Assert.assertEquals(Line.Type.USER, ul.getType());
    Assert.assertEquals("MD5", ul.getMode());
    Assert.assertEquals("USER", ul.getId());
    Assert.assertEquals("USER", ul.getUser());
    Assert.assertEquals("EMAIL", ul.getEmail());
    Assert.assertEquals(Arrays.asList("g1", "g2"), ul.getGroups());
    Assert.assertEquals(Arrays.asList("r1", "r2"), ul.getRoles());
    Assert.assertEquals(MD5UserLine.HASHER.hash("USER", "PASSWORD"), ul.getHash());
  }

  @Test
  public void testExistingUserLine() {
    String value = "USER: MD5:" + MD5UserLine.HASHER.hash("USER", "PASSWORD") + ",user,a, b";
    UserLine ul = new MD5UserLine(value);
    Assert.assertEquals(Line.Type.USER, ul.getType());
    Assert.assertEquals("MD5", ul.getMode());
    Assert.assertEquals("USER", ul.getId());
    Assert.assertEquals("USER", ul.getUser());
    Assert.assertEquals(Arrays.asList("a", "b"), ul.getRoles());
    Assert.assertEquals(MD5UserLine.HASHER.hash("USER", "PASSWORD"), ul.getHash());
  }

  @Test
  public void testSetPasswordOk() {
    String value = "USER: MD5:" + MD5UserLine.HASHER.hash("USER", "PASSWORD") + ",user,a, b";
    UserLine ul = new MD5UserLine(value);
    ul.setPassword("PASSWORD", "password");
    Assert.assertEquals(MD5UserLine.HASHER.hash("USER", "password"), ul.getHash());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetPasswordFail() {
    String value = "USER: MD5:" + MD5UserLine.HASHER.hash("USER", "PASSWORD") + ",user,a, b";
    UserLine ul = new MD5UserLine(value);
    ul.setPassword("PASSWORDX", "password");
  }

  @Test
  public void testVerifyPassword() {
    String value = "USER: MD5:" + MD5UserLine.HASHER.hash("USER", "PASSWORD") + ",user,a, b";
    UserLine ul = new MD5UserLine(value);
    Assert.assertTrue(ul.verifyPassword("PASSWORD"));
    Assert.assertFalse(ul.verifyPassword("PASSWORDX"));
  }

  @Test
  public void testResetPasswordOk() {
    String value = "USER: MD5:" + MD5UserLine.HASHER.hash("USER", "PASSWORD") + ",user,a, b";
    UserLine ul = new MD5UserLine(value);
    String reset = ul.resetPassword(1000);
    ul.setPasswordFromReset(reset, "password");
    Assert.assertEquals(MD5UserLine.HASHER.hash("USER", "password"), ul.getHash());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testResetPasswordInvalidReset() throws Exception {
    String value = "USER: MD5:" + MD5UserLine.HASHER.hash("USER", "PASSWORD") + ",user,a, b";
    UserLine ul = new MD5UserLine(value);
    String reset = ul.resetPassword(1000);
    ul.setPasswordFromReset("x" + reset, "password");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testResetPasswordExpired() throws Exception {
    String value = "USER: MD5:" + MD5UserLine.HASHER.hash("USER", "PASSWORD") + ",user,a, b";
    UserLine ul = new MD5UserLine(value);
    String reset = ul.resetPassword(1);
    Thread.sleep(2);
    ul.setPasswordFromReset(reset, "password");
  }

}
