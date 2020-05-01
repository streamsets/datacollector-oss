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
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.datacollector.security.TempKeytabManager;
import com.streamsets.pipeline.api.Configuration;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.kafka.api.KafkaDestinationGroups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KafkaKerberosUtil extends TempKeytabManager {

  private static final Logger LOG = LoggerFactory.getLogger(BaseKafkaValidationUtil.class);

  private static final String KAFKA_KEYTAB_LOCATION_KEY = "kafka.keytab.location";
  private static final String KAFKA_DEFAULT_KEYTAB_LOCATION = "/tmp/sdc";
  private static final String KAFKA_KEYTAB_SUBDIR = "kafka-keytabs";
  private static final String KAFKA_JAAS_CONFIG = "com.sun.security.auth.module.Krb5LoginModule required " +
      "useKeyTab=true keyTab=\"%s\" principal=\"%s\";";

  public KafkaKerberosUtil(Configuration configuration) throws IllegalStateException {
    super(configuration, KAFKA_KEYTAB_LOCATION_KEY, KAFKA_DEFAULT_KEYTAB_LOCATION, KAFKA_KEYTAB_SUBDIR);
  }

  public String saveUserKeytab(
      String userKeytab,
      String userPrincipal,
      Map<String, String> kafkaOptions,
      List<Stage.ConfigIssue> issues,
      Stage.Context context
  ) {
    try {
      final String keytabFileName = createTempKeytabFile(userKeytab);
      kafkaOptions.put("sasl.jaas.config", String.format(
          KAFKA_JAAS_CONFIG,
          getTempKeytabPath(keytabFileName),
          userPrincipal
      ));
      return keytabFileName;
    } catch (IOException | IllegalStateException e) {
      issues.add(context.createConfigIssue(
          KafkaDestinationGroups.KAFKA.name(),
          "userKeytab",
          KafkaErrors.KAFKA_13,
          e.getMessage(),
          e
      ));
      return null;
    }
  }

  public void deleteUserKeytabIfExists(String keytabFileName, Stage.Context context) {
    try {
      deleteTempKeytabFileIfExists(keytabFileName);
    } catch (IOException | IllegalStateException e) {
      LOG.error("Failed to delete temp keytab file {}: {}", keytabFileName, e.getMessage(), e);
    }
  }
}
