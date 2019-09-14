/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.definition;

import com.streamsets.pipeline.api.BlobStoreDef;
import com.streamsets.datacollector.config.InterceptorDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.interceptor.BaseInterceptor;
import com.streamsets.pipeline.api.interceptor.Interceptor;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;
import com.streamsets.pipeline.api.interceptor.InterceptorDef;
import org.junit.Test;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class InterceptorDefinitionExtractorTest {

  public static class DummyCreator implements InterceptorCreator {
    @Override
    public Interceptor create(Context context) {
      return null;
    }

    @Override
    public List<BlobStoreDef> blobStoreResource(BaseContext context) {
      return new LinkedList<>();
    }
  }

  @InterceptorDef(
    version = 666,
    creator = DummyCreator.class
  )
  public static class DummyInterceptor extends BaseInterceptor {
    @Override
    public List<Record> intercept(List<Record> records) {
      return records;
    }
  }

  private static final StageLibraryDefinition MOCK_LIB_DEF = new StageLibraryDefinition(
    TestLineagePublisherDefinitionExtractor.class.getClassLoader(),
    "mockity-mockity-mock",
    "REAL_MOCK",
    new Properties(),
    null,
    null,
    null
  );

  @Test
  public void textBasicExtraction() {
    InterceptorDefinition def = InterceptorDefinitionExtractor.get().extract(
      MOCK_LIB_DEF,
      DummyInterceptor.class
    );

    assertNotNull(def);
    assertEquals(getClass().getClassLoader(), def.getClassLoader());
    assertEquals(666, def.getVersion());
    assertEquals(DummyCreator.class, def.getDefaultCreator());
  }
}
