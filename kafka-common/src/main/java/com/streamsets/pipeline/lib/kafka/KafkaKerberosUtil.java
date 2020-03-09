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

public class KafkaKerberosUtil {

  private static final Logger LOG = LoggerFactory.getLogger(BaseKafkaValidationUtil.class);

  private static final String KAFKA_KEYTAB_LOCATION_KEY = "kafka.keytab.location";
  private static  final String KAFKA_DEFAULT_KEYTAB_LOCATION = "/tmp/sdc";
  private static final String KAFKA_JAAS_CONFIG = "com.sun.security.auth.module.Krb5LoginModule required " +
      "useKeyTab=true keyTab=\"%s\" principal=\"%s\";";

  public static String saveUserKeytab(
      String userKeytab,
      String userPrincipal,
      Map<String, String> kafkaOptions,
      List<Stage.ConfigIssue> issues,
      Stage.Context context
  ) {
    String keytabDir = context.getConfiguration().get(KAFKA_KEYTAB_LOCATION_KEY, KAFKA_DEFAULT_KEYTAB_LOCATION);
    Path keytabDirPath = Paths.get(keytabDir);
    String keytabFileName = UUID.randomUUID().toString();
    if (!createKeytabDirIfNeeded(keytabDirPath.toFile())) {
      addSaveException(issues, context);
    } else {
      try {
        Path keytabPath = keytabDirPath.resolve(keytabFileName);
        if (!Files.exists(keytabPath) && !keytabPath.toFile().createNewFile()) {
          LOG.debug("");
        }
        FileOutputStream writer = new FileOutputStream(keytabPath.toFile());
        writer.write(Base64.getDecoder().decode(userKeytab));
        writer.close();
        kafkaOptions.put("sasl.jaas.config", String.format(KAFKA_JAAS_CONFIG, keytabPath.toString(), userPrincipal));
      } catch (IOException ex) {
        addSaveException(issues, context);
      }
    }
    return keytabFileName;
  }

  private static synchronized boolean createKeytabDirIfNeeded(File keytabDir) {
    boolean created = keytabDir.exists();
    if (!created) {
      created = keytabDir.mkdirs();
    }
    return created;
  }

  public static void deleteUserKeytabIfExists(String keytabFileName, Stage.Context context) {
    String keytabDir = context.getConfiguration().get(KAFKA_KEYTAB_LOCATION_KEY, KAFKA_DEFAULT_KEYTAB_LOCATION);
    Path keytabDirPath = Paths.get(keytabDir);
    if (Files.exists(keytabDirPath)) {
      Path keytabPath = keytabDirPath.resolve(keytabFileName);
      if (Files.exists(keytabPath) && !keytabPath.toFile().delete()) {
        LOG.debug("Unable to delete keytab " + keytabPath.toString());
      }
    }
  }

  private static void addSaveException(List<Stage.ConfigIssue> issues, Stage.Context context) {
    issues.add(
        context.createConfigIssue(
            KafkaDestinationGroups.KAFKA.name(),
            "userKeytab",
            KafkaErrors.KAFKA_13
        )
    );
  }

}
