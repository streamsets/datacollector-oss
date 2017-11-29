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
package com.streamsets.datacollector.runner.service;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.api.service.dataformats.DataGeneratorException;

import java.io.IOException;

public class DataGeneratorServiceWrapper implements DataGenerator {

  private final ClassLoader classLoader;
  private final DataGenerator generator;

  public DataGeneratorServiceWrapper(ClassLoader classLoader, DataGenerator generator) {
    this.classLoader = Preconditions.checkNotNull(classLoader);
    this.generator = Preconditions.checkNotNull(generator);
  }

  @Override
  public void write(Record record) throws IOException, DataGeneratorException {
    LambdaUtil.privilegedWithClassLoader(classLoader,
      IOException.class, DataGeneratorException.class,
      () ->  { generator.write(record); return null;}
    );
  }

  @Override
  public void flush() throws IOException {
    LambdaUtil.privilegedWithClassLoader(
      classLoader,
      IOException.class,
      () -> { generator.flush(); return null; }
    );
  }

  @Override
  public void close() throws IOException {
    LambdaUtil.privilegedWithClassLoader(
      classLoader,
      IOException.class,
      () -> { generator.close(); return null; }
    );
  }
}
