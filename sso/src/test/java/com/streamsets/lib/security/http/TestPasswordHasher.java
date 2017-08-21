/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.lib.security.http;

import com.streamsets.datacollector.util.Configuration;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.crypto.spec.PBEKeySpec;

public class TestPasswordHasher {

  @Test
  public void testConfiguration() {
    PasswordHasher hasher = new PasswordHasher(new Configuration());
    Assert.assertEquals(PasswordHasher.V2, hasher.getCurrentVersion());
    Assert.assertEquals(100000, hasher.getIterations());
    Assert.assertEquals(256, hasher.getKeyLength());
    Assert.assertEquals(32, hasher.getSaltLength());
    Assert.assertEquals(32, hasher.getSalt().length);

    Configuration configuration = new Configuration();
    configuration.set(PasswordHasher.HASH_VERSION_KEY, PasswordHasher.V1);
    configuration.set(PasswordHasher.ITERATIONS_KEY, 1);
    configuration.set(PasswordHasher.KEY_LENGTH_KEY, 16);
    hasher = new PasswordHasher(configuration);
    Assert.assertEquals(PasswordHasher.V1, hasher.getCurrentVersion());
    Assert.assertEquals(1, hasher.getIterations());
    Assert.assertEquals(16, hasher.getKeyLength());
    Assert.assertEquals(2, hasher.getSaltLength());
    Assert.assertEquals(2, hasher.getSalt().length);
  }

  @Test
  public void testPasswordHashDefault() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(PasswordHasher.ITERATIONS_KEY, 1);
    PasswordHasher hasher = new PasswordHasher(configuration);
    String currentVersion = hasher.getCurrentVersion();

    String passwordHash = hasher.getPasswordHash("user", "foo");
    Assert.assertEquals(hasher.getCurrentVersion(), hasher.getHashVersion(passwordHash));

    Assert.assertTrue(passwordHash.startsWith(currentVersion + ":" + hasher.getIterations() + ":"));
    String[] parts = passwordHash.split(":");
    Assert.assertEquals(4, parts.length);

    int iterations = Integer.parseInt(parts[1]);
    byte[] salt = Hex.decodeHex(parts[2].toCharArray());

    PBEKeySpec spec = new PBEKeySpec(
        hasher.getValueToHash(currentVersion, "user", "foo").toCharArray(),
        salt,
        iterations,
        hasher.getKeyLength()
    );
    byte[] hash = PasswordHasher.SECRET_KEY_FACTORIES.get(hasher.getCurrentVersion()).generateSecret(spec).getEncoded();
    String hashHex = Hex.encodeHexString(hash);
    Assert.assertEquals(parts[3], hashHex);

    //valid u/p
    Assert.assertTrue(hasher.verify(passwordHash, "user", "foo"));

    // invalid u valid p, V2 catches this
    Assert.assertFalse(hasher.verify(passwordHash, "userx", "foo"));

    // invalid p
    Assert.assertFalse(hasher.verify(passwordHash, "user", "bar"));
  }

  @Test
  public void testPasswordHashV1() throws Exception {
    if (!PasswordHasher.getSupportedHashVersions().contains(PasswordHasher.V1)) {
      System.out.println("Skipping testPasswordHashV1(), no SHA512 avail");
      return;
    }
    Configuration configuration = new Configuration();
    configuration.set(PasswordHasher.ITERATIONS_KEY, 1);
    PasswordHasher hasher = new PasswordHasher(configuration);
    String passwordHash = hasher.computeHash(
        PasswordHasher.V1,
        2,
        hasher.getSalt(),
        hasher.getValueToHash(PasswordHasher.V1, "user", "foo")
    );

    //valid u/p
    Assert.assertTrue(hasher.verify(passwordHash, "user", "foo"));

    // invalid u valid p, V2 catches this
    Assert.assertTrue(hasher.verify(passwordHash, "userx", "foo"));

    // invalid p
    Assert.assertFalse(hasher.verify(passwordHash, "user", "bar"));
  }

  @Test
  public void testPasswordHashV2() throws Exception {
    if (!PasswordHasher.getSupportedHashVersions().contains(PasswordHasher.V2)) {
      System.out.println("Skipping testPasswordHashV1(), no SHA512 avail");
      return;
    }
    Configuration configuration = new Configuration();
    configuration.set(PasswordHasher.ITERATIONS_KEY, 1);
    PasswordHasher hasher = new PasswordHasher(configuration);
    String passwordHash = hasher.computeHash(
        PasswordHasher.V2,
        2,
        hasher.getSalt(),
        hasher.getValueToHash(PasswordHasher.V2, "user", "foo")
    );

    //valid u/p
    Assert.assertTrue(hasher.verify(passwordHash, "user", "foo"));

    // invalid u valid p
    Assert.assertFalse(hasher.verify(passwordHash, "userx", "foo"));

    // invalid p
    Assert.assertFalse(hasher.verify(passwordHash, "user", "bar"));
  }

  @Test
  public void testGetRandomValueGeneration() throws Exception {
    Configuration conf = new Configuration();
    conf.set(PasswordHasher.ITERATIONS_KEY, 1);
    PasswordHasher hasher = new PasswordHasher(conf);
    String[] random = hasher.getRandomValueAndHash();
    Assert.assertNotNull(random);
    Assert.assertEquals(2, random.length);
    Assert.assertNotNull(random[0]);
    Assert.assertNotNull(random[1]);
    hasher.verify(random[1], random[0], random[0]);
  }

  @Test
  public void testVerifyCaching() throws Exception {
    Configuration conf = new Configuration();
    conf.set(PasswordHasher.ITERATIONS_KEY, 1);
    PasswordHasher hasher = Mockito.spy(new PasswordHasher(conf));
    String[] valueHash = hasher.getRandomValueAndHash();

    Mockito.reset(hasher);

    // not in cache
    Assert.assertTrue(hasher.verify(valueHash[1], valueHash[0], valueHash[0]));
    Mockito.verify(hasher, Mockito.times(2)).getVerifyCache();
    Mockito
        .verify(hasher, Mockito.times(1))
        .computeHash(Mockito.eq(PasswordHasher.V2), Mockito.anyInt(), Mockito.any(byte[].class), Mockito.anyString());

    Mockito.reset(hasher);
    // in cache
    Assert.assertTrue(hasher.verify(valueHash[1], valueHash[0], valueHash[0]));
    Mockito.verify(hasher, Mockito.times(1)).getVerifyCache();
    Mockito
        .verify(hasher, Mockito.times(0))
        .computeHash(Mockito.eq("v2"), Mockito.anyInt(), Mockito.any(byte[].class), Mockito.anyString());

  }

}
