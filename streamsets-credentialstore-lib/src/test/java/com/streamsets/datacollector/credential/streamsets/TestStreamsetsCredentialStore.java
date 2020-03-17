/*
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
package com.streamsets.datacollector.credential.streamsets;

import com.streamsets.datacollector.credential.javakeystore.TestAbstractJavaKeyStoreCredentialStore;
import com.streamsets.lib.security.SshUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

public class TestStreamsetsCredentialStore extends TestAbstractJavaKeyStoreCredentialStore<StreamsetsCredentialStore> {
  @Before
  public void setUp() throws Exception {
    store = Mockito.spy(new StreamsetsCredentialStore());
    jksManager = Mockito.spy(store.new JKSManager());
    Whitebox.setInternalState(store, "manager", jksManager);
    Mockito.doReturn(jksManager).when(store).createManager();

    Mockito.doReturn(null).when(jksManager).getEntry(Matchers.eq(StreamsetsCredentialStore.SSH_PRIVATE_KEY_SECRET));
    Mockito.doNothing().when(store).generateDefaultSshKeyInfo();
  }

  @Override
  public void testInitOkRelativePath() throws Exception {
    super.testInitOkRelativePath();
    Mockito.verify(store, Mockito.times(1)).generateDefaultSshKeyInfo();
  }

  @Override
  public void testInitOkAbsolutePath() throws Exception {
    super.testInitOkAbsolutePath();
    Mockito.verify(store, Mockito.times(1)).generateDefaultSshKeyInfo();
  }

  @Test
  public void testGenerateDefaultSshKeyInfo() throws Exception {
    SshUtils.SshKeyInfoBean sshKeyInfo = new SshUtils.SshKeyInfoBean();
    sshKeyInfo.setPrivateKey("priv");
    sshKeyInfo.setPassword("pass");
    sshKeyInfo.setPublicKey("pub");

    Mockito.doCallRealMethod().when(store).generateDefaultSshKeyInfo();
    Mockito.doReturn(sshKeyInfo).when(store).createSshKeyInfo();

    Mockito.doNothing().when(store).store(Matchers.anyListOf(String.class), Matchers.eq(StreamsetsCredentialStore.SSH_PRIVATE_KEY_SECRET), Matchers.eq("priv"));
    Mockito.doNothing().when(store).store(Matchers.anyListOf(String.class), Matchers.eq(StreamsetsCredentialStore.SSH_PRIVATE_KEY_PASSWORD_SECRET), Matchers.eq("pass"));
    Mockito.doNothing().when(store).store(Matchers.anyListOf(String.class), Matchers.eq(StreamsetsCredentialStore.SSH_PUBLIC_KEY_SECRET), Matchers.eq("pub"));
    store.generateDefaultSshKeyInfo();
    Mockito.verify(store, Mockito.times(1)).store(Matchers.anyListOf(String.class), Matchers.eq(StreamsetsCredentialStore.SSH_PRIVATE_KEY_SECRET), Matchers.eq("priv"));
    Mockito.verify(store, Mockito.times(1)).store(Matchers.anyListOf(String.class), Matchers.eq(StreamsetsCredentialStore.SSH_PRIVATE_KEY_PASSWORD_SECRET), Matchers.eq("pass"));
    Mockito.verify(store, Mockito.times(1)).store(Matchers.anyListOf(String.class), Matchers.eq(StreamsetsCredentialStore.SSH_PUBLIC_KEY_SECRET), Matchers.eq("pub"));
  }


}
