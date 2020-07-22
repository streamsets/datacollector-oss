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
package com.streamsets.datacollector.security.kafka;

import com.streamsets.datacollector.security.TempKeytabManager;
import com.streamsets.pipeline.api.Configuration;
import com.streamsets.pipeline.api.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link TempKeytabManager} that is geared towards managing temporary keytabs to be used by
 * Kafka stages. Mostly a thin wrapper around its parent class, but also has a bit of logic to set some Kafka specific
 * properties from the temp keytab files that are created.
 */
public class KafkaKerberosUtil extends TempKeytabManager {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaKerberosUtil.class);

  public static final String KAFKA_KEYTAB_LOCATION_KEY = "kafka.keytab.location";
  public static final String KAFKA_DEFAULT_KEYTAB_LOCATION = "/tmp/sdc";
  public static final String KAFKA_KEYTAB_SUBDIR = "kafka-keytabs";

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
          "KAFKA",
          "userKeytab",
          KafkaKeytabErrors.KAFKA_KEYTAB_01,
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
