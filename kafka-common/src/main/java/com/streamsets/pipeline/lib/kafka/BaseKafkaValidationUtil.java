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
package com.streamsets.pipeline.lib.kafka;

import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.kafka.api.KafkaDestinationGroups;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtil;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseKafkaValidationUtil implements SdcKafkaValidationUtil {

  private static final Logger LOG = LoggerFactory.getLogger(BaseKafkaValidationUtil.class);

  @Override
  public List<HostAndPort> validateKafkaBrokerConnectionString(
    List<Stage.ConfigIssue> issues,
    String connectionString,
    String configGroupName,
    String configName,
    Stage.Context context
  ) {
    List<HostAndPort> kafkaBrokers = new ArrayList<>();
    if (connectionString == null || connectionString.isEmpty()) {
      issues.add(context.createConfigIssue(configGroupName, configName,
        KafkaErrors.KAFKA_06, configName));
    } else {
      String[] brokers = connectionString.split(",");
      for (String broker : brokers) {
        try {
          HostAndPort hostAndPort = HostAndPort.fromString(broker);
          if(!hostAndPort.hasPort() || hostAndPort.getPort() < 0) {
            issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_07, connectionString));
          } else {
            kafkaBrokers.add(hostAndPort);
          }
        } catch (IllegalArgumentException e) {
          issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_07, connectionString));
        }
      }
    }
    return kafkaBrokers;
  }

  @Override
  public List<HostAndPort> validateZkConnectionString(
      List<Stage.ConfigIssue> issues,
      String connectString,
      String configGroupName,
      String configName,
      Stage.Context context
  ) {
    List<HostAndPort> kafkaBrokers = new ArrayList<>();
    if (connectString == null || connectString.isEmpty()) {
      issues.add(context.createConfigIssue(configGroupName, configName,
        KafkaErrors.KAFKA_06, configName));
      return kafkaBrokers;
    }

    String chrootPath;
    int off = connectString.indexOf('/');
    if (off >= 0) {
      chrootPath = connectString.substring(off);
      // ignore a single "/". Same as null. Anything longer must be validated
      if (chrootPath.length() > 1) {
        try {
          PathUtils.validatePath(chrootPath);
        } catch (IllegalArgumentException e) {
          LOG.error(KafkaErrors.KAFKA_09.getMessage(), connectString, e.toString(), e);
          issues.add(context.createConfigIssue(configGroupName, configName, KafkaErrors.KAFKA_09, connectString,
            e.toString()));
        }
      }
      connectString = connectString.substring(0, off);
    }

    String brokers[] = connectString.split(",");
    for(String broker : brokers) {
      try {
        HostAndPort hostAndPort = HostAndPort.fromString(broker);
        if(!hostAndPort.hasPort() || hostAndPort.getPort() < 0) {
          issues.add(
            context.createConfigIssue(
              configGroupName,
              configName,
              KafkaErrors.KAFKA_09,
              connectString,
              "Valid port must be specified"
            )
          );
        } else {
          kafkaBrokers.add(hostAndPort);
        }
      } catch (IllegalArgumentException e) {
        LOG.error(KafkaErrors.KAFKA_09.getMessage(), connectString, e.toString(), e);
        issues.add(
            context.createConfigIssue(
                configGroupName,
                configName,
                KafkaErrors.KAFKA_09,
                connectString,
                e.toString()
            )
        );
      }
    }
    return kafkaBrokers;
  }

  public boolean isProvideKeytabAllowed(List<Stage.ConfigIssue> issues, Stage.Context context) {
    if (!isProvideKeytabSupported()) {
      issues.add(
          context.createConfigIssue(
              KafkaDestinationGroups.KAFKA.name(),
              "provideKeytab",
              KafkaErrors.KAFKA_12
          )
      );
    }
    return isProvideKeytabSupported();
  }

  protected boolean isProvideKeytabSupported() {
    return false;
  };

}
