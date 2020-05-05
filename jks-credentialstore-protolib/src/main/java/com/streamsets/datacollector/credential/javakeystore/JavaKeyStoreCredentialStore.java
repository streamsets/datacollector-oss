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

import com.streamsets.datacollector.credential.javakeystore.AbstractJavaKeyStoreCredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;

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
public class JavaKeyStoreCredentialStore extends AbstractJavaKeyStoreCredentialStore {

}
