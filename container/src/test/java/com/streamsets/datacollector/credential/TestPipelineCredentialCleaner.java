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
package com.streamsets.datacollector.credential;

import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.credential.ManagedCredentialStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class TestPipelineCredentialCleaner {

  private ManagedCredentialStore managedCredentialStore;

  static class MyManagedCredentialStore implements ManagedCredentialStore {

    static class MyCredentialValue implements CredentialValue {
      String value;

      MyCredentialValue(String value) {
        this.value = value;
      }

      @Override
      public String get() throws StageException {
        return value;
      }
    }

    private Map<String, String> inMemoryCredentialMap = new HashMap<>();

    @Override
    public void store(List<String> groups, String name, String credentialValue) throws StageException {
      inMemoryCredentialMap.put(name, credentialValue);
    }

    @Override
    public void delete(String name) throws StageException {
      inMemoryCredentialMap.remove(name);
    }

    @Override
    public List<String> getNames() throws StageException {
      return new ArrayList<>(inMemoryCredentialMap.keySet());
    }

    @Override
    public List<ConfigIssue> init(Context context) {
      return null;
    }

    @Override
    public CredentialValue get(
        String group,
        String name,
        String credentialStoreOptions
    ) throws StageException {
      return new MyCredentialValue(inMemoryCredentialMap.get(name));
    }

    @Override
    public void destroy() {
        inMemoryCredentialMap.clear();
    }
  }

  @Before
  public void setup() {
    this.managedCredentialStore = Mockito.spy(new MyManagedCredentialStore());
  }

  @Test
  public void testOnStateChangeForDelete() throws Exception {
    String pipelineId = UUID.randomUUID().toString();
    String credentialValue = "secret";

    managedCredentialStore.store(
        CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,
        CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + pipelineId + "/stage_config1",
        credentialValue
    );

    managedCredentialStore.store(
        CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,
        CredentialStoresTask.SSH_PUBLIC_KEY_SECRET,
        "publicKey"
    );

    String anotherPipelineId = UUID.randomUUID().toString();
    managedCredentialStore.store(
        CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,
        CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + anotherPipelineId + "/stage_config1",
        credentialValue
    );

    PipelineState fromState = Mockito.mock(PipelineState.class);
    Mockito.when(fromState.getStatus()).thenReturn(PipelineStatus.EDITED);
    Mockito.when(fromState.getPipelineId()).thenReturn(pipelineId);

    PipelineState toState = Mockito.mock(PipelineState.class);
    Mockito.when(toState.getStatus()).thenReturn(PipelineStatus.DELETED);
    Mockito.when(toState.getPipelineId()).thenReturn(pipelineId);

    PipelineCredentialCleaner cleaner = Mockito.spy(new PipelineCredentialCleaner(managedCredentialStore));
    cleaner.onStateChange(fromState, toState, "", null, Collections.emptyMap());

    //SSH public key present and out of 2 pipelines 1 is deleted and anotherPipelineId still is not deleted
    Assert.assertEquals(2, managedCredentialStore.getNames().size());
    List<String> names = managedCredentialStore.getNames();
    Assert.assertTrue(
        names.containsAll(
            ImmutableSet.of(
                CredentialStoresTask.SSH_PUBLIC_KEY_SECRET,
                CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + anotherPipelineId + "/stage_config1"
            )
        )
    );

    fromState = Mockito.mock(PipelineState.class);
    Mockito.when(fromState.getStatus()).thenReturn(PipelineStatus.EDITED);
    Mockito.when(fromState.getPipelineId()).thenReturn(anotherPipelineId);

    toState = Mockito.mock(PipelineState.class);
    Mockito.when(toState.getStatus()).thenReturn(PipelineStatus.DELETED);
    Mockito.when(toState.getPipelineId()).thenReturn(anotherPipelineId);

    cleaner.onStateChange(fromState, toState, "", null, Collections.emptyMap());

    //only ssh public key
    Assert.assertEquals(1, managedCredentialStore.getNames().size());
    names = managedCredentialStore.getNames();
    Assert.assertTrue(names.contains(CredentialStoresTask.SSH_PUBLIC_KEY_SECRET));

    //non existing pipeline secrets
    String nonExistingPipelineId = "nonExisting";
    fromState = Mockito.mock(PipelineState.class);
    Mockito.when(fromState.getStatus()).thenReturn(PipelineStatus.EDITED);
    Mockito.when(fromState.getPipelineId()).thenReturn(nonExistingPipelineId);

    toState = Mockito.mock(PipelineState.class);
    Mockito.when(toState.getStatus()).thenReturn(PipelineStatus.DELETED);
    Mockito.when(toState.getPipelineId()).thenReturn(nonExistingPipelineId);

    Assert.assertEquals(1, managedCredentialStore.getNames().size());
    names = managedCredentialStore.getNames();
    Assert.assertTrue(names.contains(CredentialStoresTask.SSH_PUBLIC_KEY_SECRET));
  }

  @Test
  public void testOnStateChangeNonDelete() throws Exception {
    String pipelineId = UUID.randomUUID().toString();
    String anotherPipelineId = UUID.randomUUID().toString();

    String credentialValue = "secret";

    managedCredentialStore.store(
        CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,
        CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + pipelineId + "/stage_config1",
        credentialValue
    );

    managedCredentialStore.store(
        CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,
        CredentialStoresTask.SSH_PUBLIC_KEY_SECRET,
        "publicKey"
    );

    managedCredentialStore.store(
        CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,
        CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + anotherPipelineId + "/stage_config1",
        credentialValue
    );

    PipelineCredentialCleaner cleaner = Mockito.spy(new PipelineCredentialCleaner(managedCredentialStore));

    PipelineState fromState = Mockito.mock(PipelineState.class);
    Mockito.when(fromState.getStatus()).thenReturn(PipelineStatus.EDITED);
    Mockito.when(fromState.getPipelineId()).thenReturn(pipelineId);

    for (PipelineStatus toStatus : PipelineStatus.values()) {
      if (toStatus != PipelineStatus.DELETED) {
        PipelineState toState = Mockito.mock(PipelineState.class);
        Mockito.when(toState.getStatus()).thenReturn(toStatus);
        Mockito.when(toState.getPipelineId()).thenReturn(pipelineId);
        cleaner.onStateChange(fromState, toState, "", null, Collections.emptyMap());

        Assert.assertEquals(3, managedCredentialStore.getNames().size());
        List<String> names = managedCredentialStore.getNames();
        Assert.assertTrue(
            names.containsAll(
                ImmutableSet.of(
                    CredentialStoresTask.SSH_PUBLIC_KEY_SECRET,
                    CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + pipelineId + "/stage_config1",
                    CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + anotherPipelineId + "/stage_config1"
                )
            )
        );
      }
    }
  }
}
