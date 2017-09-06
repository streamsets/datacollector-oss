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
package com.streamsets.datacollector.credential.cyberark;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

public class TestCyberArkCredentialStore {

  @Test
  public void testCreatFetcher() throws StageException, InterruptedException {
    CyberArkCredentialStore store = new CyberArkCredentialStore();

    Configuration configuration = Mockito.mock(Configuration.class);

    // webservice
    Mockito.when(configuration.get(Mockito.eq(CyberArkCredentialStore.CYBERARK_CONNECTOR_PROP), Mockito.anyString()))
           .thenReturn(CyberArkCredentialStore.CYBERARK_CONNECTOR_WEB_SERVICES);
    Assert.assertTrue(store.createFetcher(configuration) instanceof WebServicesFetcher);

    // local
    Mockito.when(configuration.get(Mockito.eq(CyberArkCredentialStore.CYBERARK_CONNECTOR_PROP), Mockito.anyString()))
           .thenReturn(CyberArkCredentialStore.CYBERARK_CONNECTOR_LOCAL);
    Assert.assertTrue(store.createFetcher(configuration) instanceof LocalFetcher);

    // invalid
    try {
      Mockito.when(configuration.get(Mockito.eq(CyberArkCredentialStore.CYBERARK_CONNECTOR_PROP), Mockito.anyString()))
             .thenReturn("invalid");
      store.createFetcher(configuration);
      Assert.fail();
    } catch (RuntimeException ex) {
    }
  }

  @Test
  public void testLifeCycle() throws StageException, InterruptedException {
    CyberArkCredentialStore store = new CyberArkCredentialStore();
    store = Mockito.spy(store);

    Fetcher fetcher = Mockito.mock(Fetcher.class);
    Mockito.doReturn(fetcher).when(store).createFetcher(Mockito.any(Configuration.class));

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getConfig(CyberArkCredentialStore.CACHE_EXPIRATION_PROP)).thenReturn("1");
    Mockito.when(context.getConfig(CyberArkCredentialStore.CREDENTIAL_REFRESH_PROP)).thenReturn("2");
    Mockito.when(context.getConfig(CyberArkCredentialStore.CREDENTIAL_RETRY_PROP)).thenReturn("3");
    Mockito.when(context.getConfig(CyberArkCredentialStore.CYBERARK_CONNECTOR_PROP)).thenReturn("webservice");
    Mockito.when(context.getConfig(CyberArkCredentialStore.CACHE_EXPIRATION_PROP)).thenReturn("1");

    store.init(context);
    Assert.assertEquals(1L, store.getCacheExpirationMillis());
    Assert.assertEquals(2L, store.getCredentialRefreshMillis());
    Assert.assertEquals(3L, store.getCredentialRetryMillis());
    Mockito.verify(fetcher, Mockito.times(1)).init(Mockito.any(Configuration.class));
    Assert.assertNotNull(store.getCache());

    store.destroy();
    Mockito.verify(fetcher, Mockito.times(1)).destroy();
  }

  @Test
  public void testCache() throws StageException, InterruptedException {
    CyberArkCredentialStore store = new CyberArkCredentialStore();
    store = Mockito.spy(store);

    Fetcher fetcher = Mockito.mock(Fetcher.class);
    Map<String, String> options = ImmutableMap.of("a", "A", "b", "B");
    Mockito.when(fetcher.fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options))).thenReturn("secret");
    Mockito.doReturn(fetcher).when(store).createFetcher(Mockito.any(Configuration.class));

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Mockito.when(context.getConfig(Mockito.eq(CyberArkCredentialStore.CACHE_EXPIRATION_PROP))).thenReturn("200");

    Assert.assertTrue(store.init(context).isEmpty());
    CredentialValue credential1 = store.get("g", "n", "a=A,b=B");
    Assert.assertNotNull(credential1);
    Assert.assertEquals("secret", credential1.get());

    //within cache time
    CredentialValue credential2 = store.get("g", "n", "a=A,b=B");
    Assert.assertSame(credential1, credential2);

    Thread.sleep(201);
    //outside cache time.
    CredentialValue credential3 = store.get("g", "n", "a=A,b=B");
    Assert.assertNotSame(credential1, credential3);

    store.destroy();
  }

  @Test
  public void testEncodeDecode() {
    CyberArkCredentialStore store = new CyberArkCredentialStore();
    store = Mockito.spy(store);
    Mockito.doReturn(Mockito.mock(CredentialStore.Context.class)).when(store).getContext();

    Assert.assertEquals("g" +
                        CyberArkCredentialStore.DELIMITER_FOR_CACHE_KEY +
                        "n" +
                        CyberArkCredentialStore.DELIMITER_FOR_CACHE_KEY +
                        "o", store.encode("g", "n", "o"));
    Assert.assertArrayEquals(new String[]{"g", "n", "o"},
        store.decode("g" +
                     CyberArkCredentialStore.DELIMITER_FOR_CACHE_KEY +
                     "n" +
                     CyberArkCredentialStore.DELIMITER_FOR_CACHE_KEY +
                     "o")
    );
  }

  @Test
  public void testCyberArkCredentialValueOptions() throws StageException {
    CyberArkCredentialStore store = new CyberArkCredentialStore();
    store = Mockito.spy(store);

    Fetcher fetcher = Mockito.mock(Fetcher.class);
    Mockito.doReturn(fetcher).when(store).createFetcher(Mockito.any(Configuration.class));

    CredentialStore.Context context = Mockito.mock(CredentialStore.Context.class);
    Assert.assertTrue(store.init(context).isEmpty());

    CredentialValue c = store.get("g", "n", "refresh=1,retry=2");
    Assert.assertNotNull(c);
    CyberArkCredentialStore.CyberArkCredentialValue cc = (CyberArkCredentialStore.CyberArkCredentialValue) c;
    Assert.assertEquals(1L, cc.getRefreshMillis());
    Assert.assertEquals(2L, cc.getRetryMillis());

    store.destroy();
  }

  @Test
  public void testCyberArkCredentialValue() throws StageException {
    CyberArkCredentialStore store = new CyberArkCredentialStore();
    store = Mockito.spy(store);
    Mockito.doReturn(Mockito.mock(CredentialStore.Context.class)).when(store).getContext();

    Fetcher fetcher = Mockito.mock(Fetcher.class);
    Map<String, String> options = ImmutableMap.of("a", "A", "b", "B");
    Mockito.when(fetcher.fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options))).thenReturn("secret");
    Mockito.doReturn(fetcher).when(store).getFetcher();

    Mockito.doReturn(2000L).when(store).getCredentialRefreshMillis();
    Mockito.doReturn(1000L).when(store).getCredentialRetryMillis();

    Mockito.doReturn(10000L).when(store).now();

    String encoded = store.encode("g", "n", "a=A, b=B");

    CredentialValue credential = store.createCredentialValue(encoded);

    // first get
    Assert.assertEquals("secret", credential.get());
    Mockito.verify(fetcher, Mockito.times(1)).fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options));

    // get already cached
    Assert.assertEquals("secret", credential.get());
    Mockito.verify(fetcher, Mockito.times(1)).fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options));

    // get already cached, with refresh
    Mockito.doReturn(12001L).when(store).now();
    Assert.assertEquals("secret", credential.get());
    Mockito.verify(fetcher, Mockito.times(2)).fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options));

    // get fail
    Mockito.doReturn(14002L).when(store).now();
    Mockito.when(fetcher.fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options)))
           .thenThrow(new StageException(Errors.CYBERARCK_001));
    try {
      credential.get();
      Assert.fail();
    } catch (StageException ex) {
      Mockito.verify(fetcher, Mockito.times(3)).fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options));
    }

    // get fail, within retry interval
    try {
      credential.get();
      Assert.fail();
    } catch (StageException ex) {
      Mockito.verify(fetcher, Mockito.times(3)).fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options));
    }

    // get fail, after retry interval
    Mockito.doReturn(15003L).when(store).now();
    try {
      credential.get();
      Assert.fail();
    } catch (StageException ex) {
      Mockito.verify(fetcher, Mockito.times(4)).fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options));
    }

    // recover
    Mockito.when(fetcher.fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options))).thenReturn("secret");
    Mockito.doReturn(16004L).when(store).now();
    Assert.assertEquals("secret", credential.get());
    Mockito.verify(fetcher, Mockito.times(5)).fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options));

    // get already cached, it should use refresh interval now
    Mockito.doReturn(17004L).when(store).now();
    Assert.assertEquals("secret", credential.get());
    Mockito.verify(fetcher, Mockito.times(5)).fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options));

    // get already cached, with refresh
    Mockito.doReturn(18005L).when(store).now();
    Assert.assertEquals("secret", credential.get());
    Mockito.verify(fetcher, Mockito.times(6)).fetch(Mockito.eq("g"), Mockito.eq("n"), Mockito.eq(options));
  }

  @Test
  public void testCacheEncodeDecode() {
    CyberArkCredentialStore store = new CyberArkCredentialStore();
    store = Mockito.spy(store);
    Mockito.doReturn(Mockito.mock(CredentialStore.Context.class)).when(store).getContext();

    String encoded = store.encode("g", "n", "o");
    Assert.assertArrayEquals(new String[]{"g", "n", "o"}, store.decode(encoded));
    encoded = store.encode("g", "n", "");
    Assert.assertArrayEquals(new String[]{"g", "n", ""}, store.decode(encoded));
  }

}
