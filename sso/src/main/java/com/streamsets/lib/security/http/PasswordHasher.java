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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class PasswordHasher {
  private static final Logger LOG = LoggerFactory.getLogger(PasswordHasher.class);

  public static final String RANDOM_ALGORITHM = "SHA1PRNG";

  public static final String HASH_ALGORITHM_V1_V2 = "PBKDF2WithHmacSHA512";

  public static final String HASH_ALGORITHM_V3 = "PBKDF2WithHmacSHA1";

  static final Map<String, SecretKeyFactory> SECRET_KEY_FACTORIES;

  private static final SecureRandom SECURE_RANDOM;

  // hashes password only
  public static final String V1 = "v1";

  // hashes (user + password), to avoid swap-ability of passwords in storage
  public static final String V2 = "v2";

  // V2 but using SHA1 instead of SHA512. Deprecated since Java 7 is no longer supported.
  @Deprecated
  public static final String V3 = "v3";

  static {
    try {
      SECURE_RANDOM = SecureRandom.getInstance(RANDOM_ALGORITHM);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    Map<String, SecretKeyFactory> map = new HashMap<>();
    try {
      SecretKeyFactory factory = SecretKeyFactory.getInstance(HASH_ALGORITHM_V1_V2);
      map.put(V1, factory);
      map.put(V2, factory);
    } catch (Exception ex) {
      LOG.warn("Algorithm '{}' not available, v1 and v2 hashes are not supported", HASH_ALGORITHM_V1_V2);
    }

    try {
      SecretKeyFactory factory = SecretKeyFactory.getInstance(HASH_ALGORITHM_V3);
      map.put(V3, factory);
    } catch (Exception ex) {
      LOG.warn("Algorithm '{}' not available, v3 hashes are not supported", HASH_ALGORITHM_V3);
    }

    if (map.isEmpty()) {
      throw new RuntimeException("There is no hash algorithm available");
    }
    SECRET_KEY_FACTORIES = ImmutableMap.copyOf(map);
  }

  public static Set<String> getSupportedHashVersions() {
    return SECRET_KEY_FACTORIES.keySet();
  }

  public static final String CONFIG_PREFIX = "passwordHandler.";

  public static final String HASH_VERSION_KEY = CONFIG_PREFIX + "hashVersion";
  public static final String HASH_VERSION_DEFAULT = V2;

  public static final String ITERATIONS_KEY = CONFIG_PREFIX + "iterations";
  public static final int ITERATIONS_DEFAULT = 100000;

  public static final String KEY_LENGTH_KEY = CONFIG_PREFIX + "keyLength";
  public static final int KEY_LENGTH_DEFAULT = 256;

  private final String hashVersion;
  private final int iterations;
  private final int keyLength;
  private final Cache<String, String> verifyCache;

  public PasswordHasher(Configuration configuration) {
    hashVersion = configuration.get(HASH_VERSION_KEY, HASH_VERSION_DEFAULT);
    iterations = configuration.get(ITERATIONS_KEY, ITERATIONS_DEFAULT);
    keyLength = configuration.get(KEY_LENGTH_KEY, KEY_LENGTH_DEFAULT);
    // expire on access of 20mins it is 40 times over the validation time.
    verifyCache = CacheBuilder.newBuilder().expireAfterAccess(20, TimeUnit.MINUTES).build();
  }

  public String[] getRandomValueAndHash() {
    byte[] random = new byte[64];
    SECURE_RANDOM.nextBytes(random);
    String value = Hex.encodeHexString(random);
    String hash = getPasswordHash(value, value);
    return new String[]{value, hash};
  }

  @VisibleForTesting
  Cache<String, String> getVerifyCache() {
    return verifyCache;
  }

  public String getPasswordHash(String user, String password) {
    int iterations = getIterations();
    byte[] salt = getSalt();
    return computeHash(hashVersion, iterations, salt, getValueToHash(hashVersion, user, password));
  }

  protected String getValueToHash(String hashVersion, String user, String password) {
    switch (hashVersion) {
      case V1:
        return password;
      case V2:
      case V3:
        return user + "\n" + password;
      default:
        throw new IllegalArgumentException(Utils.format("Invalid/unsupported hash version '{}'", hashVersion));
    }
  }

  protected String computeHash(String version, int iterations, byte[] salt, String valueTohash) {
    long start = System.currentTimeMillis();
    try {
      // yield CPU when this method is run in a tight loop
      Thread.yield();
      PBEKeySpec spec = new PBEKeySpec(valueTohash.toCharArray(), salt, iterations, getKeyLength());
      byte[] hash = SECRET_KEY_FACTORIES.get(version).generateSecret(spec).getEncoded();
      return version + ":" + iterations + ":" + Hex.encodeHexString(salt) + ":" + Hex.encodeHexString(hash);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      LOG.trace(
          "Computing password hash '{}' with '{}' iterations took '{}msec'",
          version,
          iterations,
          System.currentTimeMillis() - start
      );
    }
  }

  protected String getHashVersion(String hash) {
    String version = "UNKNOWN";
    int idx = hash.indexOf(":");
    if (idx > -1) {
      version = hash.substring(0, idx);
    }
    return version;
  }

  public boolean verify(String storedPasswordHash, String user, String givenPassword) {
    boolean ok = false;
    String version = getHashVersion(storedPasswordHash);
    String valueToHash = getValueToHash(version, user, givenPassword);
    String cachedPassword = getVerifyCache().getIfPresent(storedPasswordHash);
    if (cachedPassword != null) {
      ok = cachedPassword.equals(valueToHash);
    } else {
      try {
        String[] parts = storedPasswordHash.split(":");
        if (parts.length > 0) {
          switch (version) {
            case V1:
            case V2:
            case V3: {
              if (parts.length == 4) {
                int hashIterations = Integer.parseInt(parts[1]);
                byte[] hashSalt = Hex.decodeHex(parts[2].toCharArray());
                // we don't need the stored hash (parts[3]) as we compare the fully stored thing

                String recomputedHash = computeHash(version, hashIterations, hashSalt, valueToHash);
                ok = storedPasswordHash.equals(recomputedHash);
                if (ok) {
                  getVerifyCache().put(storedPasswordHash, valueToHash);
                }
              }
            }
            break;
            default:
              throw new IllegalArgumentException(Utils.format("Invalid/unsupported hash version '{}'", version));
          }
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return ok;
  }

  public String getCurrentVersion() {
    return hashVersion;
  }

  protected int getIterations() {
    return iterations;
  }

  protected int getKeyLength() {
    return keyLength;
  }

  protected int getSaltLength() {
    return getKeyLength() / 8;
  }

  protected byte[] getSalt() {
    byte[] salt = new byte[getSaltLength()];
    SECURE_RANDOM.nextBytes(salt);
    return salt;
  }

}
