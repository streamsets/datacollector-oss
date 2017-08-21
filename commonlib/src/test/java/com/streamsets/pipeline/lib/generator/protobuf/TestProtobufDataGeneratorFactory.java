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
package com.streamsets.pipeline.lib.generator.protobuf;

import com.google.common.io.Resources;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.util.ProtobufConstants;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.OutputStream;
import java.util.Collections;

public class TestProtobufDataGeneratorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TestProtobufDataGeneratorFactory.class);

  private static final String PROTO_FILE = "Employee.desc";
  private static final String MESSAGE_TYPE = "util.Employee";

  @Test
  public void testGetFactory() throws Exception {
    DataGeneratorFactoryBuilder dataGenFactoryBuilder = new DataGeneratorFactoryBuilder(getContext(),
      DataGeneratorFormat.PROTOBUF);
    DataGeneratorFactory factory = dataGenFactoryBuilder
      .setConfig(ProtobufConstants.PROTO_DESCRIPTOR_FILE_KEY, Resources.getResource(PROTO_FILE).getPath())
      .setConfig(ProtobufConstants.MESSAGE_TYPE_KEY, MESSAGE_TYPE)
      .build();

    Assert.assertNotNull(factory);
    Assert.assertTrue(factory instanceof ProtobufDataGeneratorFactory);

    DataGenerator generator = factory.getGenerator((OutputStream)null);

    Assert.assertNotNull(generator);
    Assert.assertTrue(generator instanceof ProtobufDataGenerator);

  }

  @Test
  public void testGetFactoryWithNonExistingType() {
    DataGeneratorFactoryBuilder dataGenFactoryBuilder = new DataGeneratorFactoryBuilder(getContext(),
      DataGeneratorFormat.PROTOBUF);

    try {
      dataGenFactoryBuilder
        .setConfig(ProtobufConstants.PROTO_DESCRIPTOR_FILE_KEY, Resources.getResource(PROTO_FILE).getPath())
        .setConfig(ProtobufConstants.MESSAGE_TYPE_KEY, "Hello")
        .build();
      Assert.fail("Exception expected as the message type does not exist in the descriptor file");
    } catch (RuntimeException e) {
      LOG.debug("Ignoring exception", e);
    }
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR,
      Collections.<String>emptyList());
  }

}
