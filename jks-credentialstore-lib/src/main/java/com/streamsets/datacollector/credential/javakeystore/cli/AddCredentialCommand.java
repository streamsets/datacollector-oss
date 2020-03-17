/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.credential.javakeystore.cli;

import com.streamsets.datacollector.credential.javakeystore.JavaKeyStoreCredentialStore;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "add", description = "Add a credential to the Java Keystore Credential Store")
public class AddCredentialCommand extends AbstractJKSCommand {

  @Option(
      name = {"-n", "--name"},
      description = "Credential name",
      required = true
  )
  public String name;

  @Option(
      name = {"-c", "--credential"},
      description = "Credential (the password to store)",
      required = true
  )
  public String credential;

  @Override
  protected void execute(JavaKeyStoreCredentialStore store) {
    store.store(JavaKeyStoreCredentialStore.DEFAULT_SDC_GROUP_AS_LIST, name, credential);
  }

}
