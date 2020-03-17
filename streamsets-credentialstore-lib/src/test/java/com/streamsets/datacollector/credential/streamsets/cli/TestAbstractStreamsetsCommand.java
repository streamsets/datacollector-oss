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
package com.streamsets.datacollector.credential.streamsets.cli;

import com.streamsets.datacollector.credential.javakeystore.cli.TestAbstractCommand;
import com.streamsets.datacollector.credential.streamsets.StreamsetsCredentialStore;
import org.junit.Before;
import org.mockito.Mockito;

public class TestAbstractStreamsetsCommand extends TestAbstractCommand<StreamsetsCredentialStore> {

  public TestAbstractStreamsetsCommand(String productName) {
    super(productName);
  }

  @Before
  public void setUp() throws Exception {
    store = Mockito.mock(StreamsetsCredentialStore.class);
    command = Mockito.spy(new AbstractStreamsetsCommand() {
      @Override
      protected void execute(StreamsetsCredentialStore store) {
      }

      @Override
      protected StreamsetsCredentialStore createStore() {
        return store;
      }
    });
  }
}
