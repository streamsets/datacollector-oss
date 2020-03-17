/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.credential.streamsets.cli;

import com.streamsets.datacollector.credential.streamsets.StreamsetsCredentialStore;
import org.junit.Test;
import org.mockito.Mockito;

public class TestDefaultSshKeyInfoCommand {

  @Test
  public void testExecute() {
    StreamsetsCredentialStore store = Mockito.mock(StreamsetsCredentialStore.class);
    DefaultSshKeyInfoCommand command = new DefaultSshKeyInfoCommand();

    command.execute(store);
    Mockito.verify(store, Mockito.times(1)).generateDefaultSshKeyInfo();
    Mockito.verifyNoMoreInteractions(store);
  }

}
