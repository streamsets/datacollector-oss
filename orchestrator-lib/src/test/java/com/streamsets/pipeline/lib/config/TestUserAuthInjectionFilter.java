/*
 * Copyright 2021  StreamSets Inc.
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
package com.streamsets.pipeline.lib.config;

import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import java.util.function.Supplier;

public class TestUserAuthInjectionFilter {

  @Test
  public void testFilter() {
    Supplier<String> supplier = Mockito.mock(Supplier.class);
    Mockito.when(supplier.get()).thenReturn("foo");
    UserAuthInjectionFilter filter = new UserAuthInjectionFilter(supplier);
    ClientRequestContext request = Mockito.mock(ClientRequestContext.class);
    MultivaluedMap<String, Object> headers = Mockito.mock(MultivaluedMap.class);
    Mockito.when(request.getHeaders()).thenReturn(headers);

    filter.filter(request);
    Mockito.verify(supplier, Mockito.times(1)).get();
    Mockito.verify(headers, Mockito.times(1)).add(Mockito.eq("X-SS-User-Auth-Token"), Mockito.eq("foo"));
    filter.filter(request);
    Mockito.verify(supplier, Mockito.times(1)).get();
  }
}
