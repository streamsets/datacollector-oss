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
package com.streamsets.datacollector.credential.javakeystore;

import com.streamsets.datacollector.io.DataStore;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Credential store backed by a Java Keystore, JCEKS or PCKS12 formats only.
 * <p/>
 * The credentials are stored as if they would be AES keys leveraging the fact that the key length is not checked.
 * <p/>
 * The store uses data collector's <code>DataStore</code> to avoid corruption on the keystore file.
 * <p/>
 * This implementation has 3 (required) configuration values:
 * <ul>
 *   <li>credentialStore.[ID].keystore.type: the type of the keystore JCEKS or PCKS12</li>
 *   <li>credentialStore.[ID].keystore.file: the keystore file. If a relative path is set,the keystore file is
 *   looked into the SDC configuration directory. If an absolute path is set, the keystore file is looked into that
 *   absolute location.</li>
 *   <li>credentialStore.[ID].keystore.storePassword: the keystore file and entries password.</li>
 * </ul>
 * <p/>
 * This implementation has additional methods, not part of the CredentialStore API, to add, delete and list credentials.
 * These methods are used by the command line tool 'streamsets jks-cs'.
 * <p/>
 * NOTE: there can be multiple credential stores of the same implementation, each one is identified an configured
 * via its ID in the configuration.
 */
@CredentialStoreDef(label = "Java KeyStore")
public class JavaKeyStoreCredentialStore implements CredentialStore {
  private static final Logger LOG = LoggerFactory.getLogger(JavaKeyStoreCredentialStore.class);
  private static final Set<String> VALID_KEYSTORE_TYPES = new HashSet<>();

  static final String KEYSTORE_TYPE_KEY = "keystore.type";
  static final String KEYSTORE_FILE_KEY = "keystore.file";
  static final String KEYSTORE_PASSWORD_KEY = "keystore.storePassword"; // NOSONAR

  static {
    VALID_KEYSTORE_TYPES.add("JCEKS");
    VALID_KEYSTORE_TYPES.add("PKCS12");
  }

  private Context context;
  private String keystoreType;
  private File keyStoreFile;
  private String keystorePassword;
  private long keystoreTimestamp;
  private volatile KeyStore keyStore;

  @Override
  public List<ConfigIssue> init(Context context) {
    List<ConfigIssue> issues = new ArrayList<>();
    this.context = context;

    keystoreType = context.getConfig(KEYSTORE_TYPE_KEY);
    if (keystoreType == null) {
      issues.add(context.createConfigIssue(Errors.JKS_CRED_STORE_000, KEYSTORE_TYPE_KEY));
    } else {
      if (!VALID_KEYSTORE_TYPES.contains(keystoreType.toUpperCase())) {
        issues.add(context.createConfigIssue(Errors.JKS_CRED_STORE_002, keystoreType));
      }
    }

    String fileName = context.getConfig(KEYSTORE_FILE_KEY);
    if (fileName == null) {
      issues.add(context.createConfigIssue(Errors.JKS_CRED_STORE_000, KEYSTORE_FILE_KEY));
    } else {
      keyStoreFile = new File(fileName);
      if (!keyStoreFile.isAbsolute()) {
        keyStoreFile = new File(System.getProperty("sdc.conf.dir"), fileName);
      }
    }

    keystorePassword = context.getConfig(KEYSTORE_PASSWORD_KEY);
    if (keystorePassword == null) {
      issues.add(context.createConfigIssue(Errors.JKS_CRED_STORE_000, KEYSTORE_PASSWORD_KEY));
    }
    if (issues.isEmpty()) {
      setKeyStore(loadKeyStore());
    }
    return issues;
  }


  protected Context getContext() {
    return context;
  }

  protected File getKeyStoreFile() {
    return keyStoreFile;
  }

  protected String getKeystoreType() {
    return keystoreType;
  }

  protected String getKeystorePassword() {
    return keystorePassword;
  }

  protected long getKeystoreTimestamp() {
    return keystoreTimestamp;
  }

  protected void setKeyStore(KeyStore keyStore) {
    this.keyStore = keyStore;
  }

  protected KeyStore getKeyStore() {
    return keyStore;
  }

  class JksCredentialValue implements CredentialValue {
    private final String name;

    public JksCredentialValue(String name) {
      this.name = name;
    }

    @Override
    public String get() throws StageException {
      String credential = null;
      if (needsToReloadKeyStore()) {
        setKeyStore(loadKeyStore());
      }
      KeyStore keyStore = getKeyStore();
      if (keyStore == null) {
        throw new StageException(Errors.JKS_CRED_STORE_003, name);
      } else {
        try {
          KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry) getKeyStore().getEntry(
              name,
              new KeyStore.PasswordProtection(getKeystorePassword().toCharArray())
          );
          if (entry != null) {
            credential = new String(entry.getSecretKey().getEncoded(), "UTF-8");
          } else {
            throw new StageException(Errors.JKS_CRED_STORE_003, name);
          }
        } catch (Exception ex) {
          throw new StageException(Errors.JKS_CRED_STORE_004, name, ex);
        }
      }
      return credential;
    }

    @Override
    public String toString() {
      return "JksCredentialValue{}";
    }

  }

  @Override
  public CredentialValue get(String group, String name, String credentialStoreOptions) throws StageException {
    Utils.checkNotNull(group, "group cannot be NULL");
    Utils.checkNotNull(name, "name cannot be NULL");
    CredentialValue credential = new JksCredentialValue(name);
    credential.get();
    return credential;
  }

  @Override
  public void destroy() {

  }

  protected KeyStore createKeyStore() {
    try {
      KeyStore keyStore = KeyStore.getInstance(getKeystoreType());
      keyStore.load(null);
      return keyStore;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  protected synchronized KeyStore loadKeyStore() {
    KeyStore keyStore = null;
    if (getKeyStoreFile().exists()) {
      keystoreTimestamp = getKeyStoreFile().lastModified();
      DataStore dataStore = new DataStore(getKeyStoreFile());
      try (InputStream is = dataStore.getInputStream()) {
        keyStore = KeyStore.getInstance(getKeystoreType());
        keyStore.load(is, getKeystorePassword().toCharArray());
        LOG.debug("Loaded keystore '{}': {}", getKeyStoreFile());
      } catch (Exception ex) {
        LOG.error("Failed to load keystore '{}': {}", getKeyStoreFile(), ex);
        keyStore = null;
      }
    }
    return keyStore;
  }

  protected long now() {
    return System.currentTimeMillis();
  }

  protected boolean needsToReloadKeyStore() {
    //the last comparision is to ensure the file has been fully flushed, some crap of linux
    return (getKeyStore() == null) ||
           ((getKeyStoreFile().lastModified() > getKeystoreTimestamp()) &&
            (now() - getKeyStoreFile().lastModified() > 10000));
  }

  public void storeCredential(String name, String credential) {
    Utils.checkNotNull(name, "name cannot be NULL");
    Utils.checkNotNull(credential, "credential cannot be NULL");

    try {
      KeyStore keyStore = loadKeyStore();
      if (keyStore == null) {
        keyStore = createKeyStore();
      }
      // the keystore does not check the length of the KEY whe adding a AES key,
      // so we just put the credential as AES key payload.
      SecretKey secret = new SecretKeySpec(credential.getBytes("UTF-8"), "AES");

      keyStore.setEntry(
          name,
          new KeyStore.SecretKeyEntry(secret),
          new KeyStore.PasswordProtection(getKeystorePassword().toCharArray())
      );

      persistStore(keyStore);

    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  protected void persistStore(KeyStore keyStore) throws IOException {
    DataStore dataStore = new DataStore(getKeyStoreFile());
    try (OutputStream os = dataStore.getOutputStream()) {
      OutputStream csos = new CloseShieldOutputStream(os);
      keyStore.store(csos, getKeystorePassword().toCharArray());
      dataStore.commit(os);
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IOException(ex);
    } finally {
      dataStore.release();
    }
  }

  public void deleteCredential(String name) {
    Utils.checkNotNull(name, "name cannot be NULL");

    try {
      KeyStore keyStore = loadKeyStore();
      if (keyStore == null) {
        keyStore = createKeyStore();
      }
      if (keyStore.getKey(name, getKeystorePassword().toCharArray()) != null) {
        keyStore.deleteEntry(name);
        persistStore(keyStore);
      } else {
        throw new IOException(Utils.format("Credential Store '{}', alias '{}' not found", getContext().getId(), name));
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public List<String> getAliases() {
    List<String> aliases = new ArrayList<>();
    try {
      KeyStore keyStore = loadKeyStore();
      if (keyStore != null) {
        Enumeration<String> it = keyStore.aliases();
        while (it.hasMoreElements()) {
          aliases.add(it.nextElement());
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return aliases;
  }

}
