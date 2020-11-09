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
package com.streamsets.datacollector.classpath;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

@RunWith(Parameterized.class)
public class DependencyParserExhaustiveTest {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object []> jarFiles() {
    return Arrays.asList(new Object[][]{
      {"HikariCP-1.3.9.jar", new Dependency("HikariCP", "1.3.9")},
      {"HikariCP-2.4.1.jar", new Dependency("HikariCP", "2.4.1")},
      {"RoaringBitmap-0.5.11.jar", new Dependency("RoaringBitmap", "0.5.11")},
      {"ST4-4.0.4.jar", new Dependency("ST4", "4.0.4")},
      {"activation-1.1.jar", new Dependency("activation", "1.1")},
      {"activemq-client-5.13.4.jar", new Dependency("activemq-client", "5.13.4")},
      {"airline-0.7.jar", new Dependency("airline", "0.7")},
      {"akka-actor_2.10-2.2.3-shaded-protobuf.jar", new Dependency("akka-actor_2.10", "2.2.3")},
      {"akka-actor_2.10-2.3.11.jar", new Dependency("akka-actor_2.10", "2.3.11")},
      {"amazon-kinesis-client-1.8.7.jar", new Dependency("amazon-kinesis-client", "1.8.7")},
      {"amazon-kinesis-producer-0.12.5.jar", new Dependency("amazon-kinesis-producer", "0.12.5")},
      {"amazon-sqs-java-messaging-lib-1.0.4.jar", new Dependency("amazon-sqs-java-messaging-lib", "1.0.4")},
      {"amqp-client-3.5.6.jar", new Dependency("amqp-client", "3.5.6")},
      {"annotations-java5-15.0.jar", new Dependency("annotations-java5", "15.0")},
      {"antlr-2.7.7.jar", new Dependency("antlr", "2.7.7")},
      {"antlr-runtime-3.4.jar", new Dependency("antlr", "3.4")},
      {"antlr4-runtime-4.5.3.jar", new Dependency("antlr4", "4.5.3")},
      {"aopalliance-repackaged-2.5.0-b32.jar", new Dependency("aopalliance-repackaged", "2.5.0-b32")},
      {"apacheds-i18n-2.0.0-M15.jar", new Dependency("apacheds-i18n", "2.0.0-M15")},
      {"apacheds-kerberos-codec-2.0.0-M15.jar", new Dependency("apacheds-kerberos-codec", "2.0.0-M15")},
      {"api-asn1-api-1.0.0-M20.jar", new Dependency("api-asn1-api", "1.0.0-M20")},
      {"api-util-1.0.0-M20.jar", new Dependency("api-util", "1.0.0-M20")},
      {"asm-3.1.jar", new Dependency("asm", "3.1")},
      {"asm-analysis-5.0.3.jar", new Dependency("asm", "5.0.3")},
      {"asm-commons-3.1.jar", new Dependency("asm", "3.1")},
      {"asm-tree-3.1.jar", new Dependency("asm", "3.1")},
      {"asm-util-5.0.3.jar", new Dependency("asm", "5.0.3")},
      {"atlas-client-0.8.0.2.6.1.0-129.jar", new Dependency("atlas", "0.8.0.2.6.1.0-129")},
      {"atlas-common-0.8.0.2.6.2.1-1.jar", new Dependency("atlas", "0.8.0.2.6.2.1-1")},
      {"atlas-intg-0.8.0.2.6.1.0-129.jar", new Dependency("atlas", "0.8.0.2.6.1.0-129")},
      {"atlas-notification-0.8.0.2.6.2.1-1.jar", new Dependency("atlas", "0.8.0.2.6.2.1-1")},
      {"atlas-server-api-0.8.0.2.6.1.0-129.jar", new Dependency("atlas", "0.8.0.2.6.1.0-129")},
      {"atlas-typesystem-0.8.0.2.6.2.1-1.jar", new Dependency("atlas", "0.8.0.2.6.2.1-1")},
      {"avatica-1.8.0.2.5.0.0-1245.jar", new Dependency("avatica", "1.8.0.2.5.0.0-1245")},
      {"avatica-metrics-1.8.0.2.6.2.1-1.jar", new Dependency("avatica", "1.8.0.2.6.2.1-1")},
      {"avro-1.7.3.jar", new Dependency("avro", "1.7.3")},
      {"avro-1.7.6-cdh5.13.0.jar", new Dependency("avro", "1.7.6-cdh5.13.0")},
      {"avro-ipc-1.7.6-cdh5.13.0-tests.jar", new Dependency("avro", "1.7.6-cdh5.13.0")},
      {"avro-ipc-1.7.7-tests.jar", new Dependency("avro", "1.7.7")},
      {"avro-mapred-1.7.6-cdh5.13.0-hadoop2.jar", new Dependency("avro", "1.7.6-cdh5.13.0")},
      {"avro-mapred-1.7.7-hadoop2.jar", new Dependency("avro", "1.7.7")},
      {"aws-java-sdk-bundle-1.11.134.jar", new Dependency("aws-java-sdk", "1.11.134")},
      {"aws-java-sdk-cloudwatch-1.11.151.jar", new Dependency("aws-java-sdk", "1.11.151")},
      {"azure-data-lake-store-sdk-2.1.5.jar", new Dependency("azure-data-lake-store-sdk", "2.1.5")},
      {"bcprov-jdk15on-1.57.jar", new Dependency("bcprov-jdk15on", "1.57")},
      {"bigtable-client-core-1.0.0-pre3.jar", new Dependency("bigtable-client-core", "1.0.0-pre3")},
      {"bigtable-hbase-1.x-1.0.0-pre3.jar", new Dependency("bigtable-hbase-1.x", "1.0.0-pre3")},
      {"bonecp-0.8.0.RELEASE.jar", new Dependency("bonecp", "0.8.0.RELEASE")},
      {"calcite-avatica-1.0.0-incubating.jar", new Dependency("calcite", "1.0.0-incubating")},
      {"calcite-core-1.0.0-incubating.jar", new Dependency("calcite", "1.0.0-incubating")},
      {"calcite-core-1.10.0.2.6.2.1-1.jar", new Dependency("calcite", "1.10.0.2.6.2.1-1")},
      {"connect-api-0.9.0.0-mapr-1707-beta.jar", new Dependency("connect-api", "0.9.0.0-mapr-1707-beta")},
      {"commons-logging-1.1.3-api.jar", new Dependency("commons-logging", "1.1.3")},
      {"db2jcc4.jar", new Dependency("db2jcc4", "")},
      {"dbml-local-0.4.1-spark2.3.jar", new Dependency("dbml-local", "0.4.1-spark2.3")},
      {"derby-10.10.2.0.jar", new Dependency("derby", "10.10.2.0")},
      {"dropwizard-metrics-hadoop-metrics2-reporter-0.1.2.jar", new Dependency("dropwizard-metrics-hadoop-metrics2-reporter", "0.1.2")},
      {"elsa-3.0.0-M5.jar", new Dependency("elsa", "3.0.0-M5")},
      {"flatbuffers-1.2.0-3f79e055.jar", new Dependency("flatbuffers", "1.2.0-3f79e055")},
      {"flume-ng-sdk-1.5.2.2.4.2.0-258.jar", new Dependency("flume-ng-sdk", "1.5.2.2.4.2.0-258")},
      {"flume-ng-sdk-1.6.0-cdh5.11.2.jar", new Dependency("flume-ng-sdk", "1.6.0-cdh5.11.2")},
      {"geronimo-annotation_1.0_spec-1.1.1.jar", new Dependency("geronimo", "1.1.1")},
      {"geronimo-j2ee-management_1.1_spec-1.0.1.jar", new Dependency("geronimo", "1.0.1")},
      {"geronimo-jms_1.1_spec-1.1.1.jar", new Dependency("geronimo", "1.1.1")},
      {"geronimo-jta_1.1_spec-1.1.1.jar", new Dependency("geronimo", "1.1.1")},
      {"google-cloud-bigquery-0.25.0-beta.jar", new Dependency("google-cloud-bigquery", "0.25.0-beta")},
      {"groovy-2.4.11-indy.jar", new Dependency("groovy", "2.4.11-indy")},
      {"groovy-all-2.4.4.jar", new Dependency("groovy", "2.4.4")},
      {"grpc-auth-1.5.0.jar", new Dependency("grpc", "1.5.0")},
      {"guava-24.1-jre.jar", new Dependency("guava", "24.1-jre")},
      {"guava-28.0-android.jar", new Dependency("guava", "28.0-android")},
      {"grpc-context-1.6.1.jar", new Dependency("grpc", "1.6.1")},
      {"grpc-google-cloud-pubsub-v1-0.1.20.jar", new Dependency("grpc-google-cloud-pubsub-v1", "0.1.20")},
      {"grpc-google-common-protos-0.1.9.jar", new Dependency("grpc-google-common-protos", "0.1.9")},
      {"hadoop-annotations-2.6.0-cdh5.10.1.jar", new Dependency("hadoop", "2.6.0-cdh5.10.1")},
      {"hadoop-annotations-2.7.0-mapr-1607.jar", new Dependency("hadoop", "2.7.0-mapr-1607")},
      {"hadoop-annotations-2.7.1.2.4.2.0-258.jar", new Dependency("hadoop", "2.7.1.2.4.2.0-258")},
      {"hamcrest-core-1.3.jar", new Dependency("hamcrest", "1.3")},
      {"hamcrest-library-1.3.jar", new Dependency("hamcrest", "1.3")},
      {"hbase-annotations-1.1.2.2.4.2.0-258.jar", new Dependency("hbase", "1.1.2.2.4.2.0-258")},
      {"hbase-client-1.2.0-cdh5.11.2.jar", new Dependency("hbase", "1.2.0-cdh5.11.2")},
      {"hive-ant-1.1.0-cdh5.10.1.jar", new Dependency("hive", "1.1.0-cdh5.10.1")},
      {"hive-bridge-0.8.0.2.6.1.0-129.jar", new Dependency("hive-bridge", "0.8.0.2.6.1.0-129")},
      {"hive-jdbc-1.2.0-mapr-1707-standalone.jar", new Dependency("hive", "1.2.0-mapr-1707-standalone")},
      {"hk2-api-2.5.0-b32.jar", new Dependency("hk2-api", "2.5.0-b32")},
      {"jackson-annotations-2.4.0.jar", new Dependency("jackson", "2.4.0")},
      {"jackson-mapper-asl-1.8.10-cloudera.1.jar", new Dependency("jackson", "1.8.10-cloudera.1")},
      {"jackson-core-2.10.0.pr1.jar", new Dependency("jackson", "2.10.0.pr1")},
      {"javassist-3.18.1-GA.jar", new Dependency("javassist", "3.18.1-GA")},
      {"javassist-3.12.1.GA.jar", new Dependency("javassist", "3.12.1.GA")},
      {"javax.jdo-3.2.0-m3.jar", new Dependency("javax.jdo", "3.2.0-m3")},
      {"javax.servlet-3.0.0.v201112011016.jar", new Dependency("javax.servlet", "3.0.0.v201112011016")},
      {"jbcrypt-0.3m.jar", new Dependency("jbcrypt", "0.3m")},
      {"JDBC-9.3.0.0206-cubrid.jar", new Dependency("JDBC", "9.3.0.0206-cubrid")},
      {"jersey-apache-connector-2.25.1.jar", new Dependency("jersey", "2.25.1")},
      {"jersey-client-1.19.jar", new Dependency("jersey", "1.19")},
      {"jetty-6.1.26.cloudera.4.jar", new Dependency("jetty", "6.1.26.cloudera.4")},
      {"jetty-6.1.26.hwx.jar", new Dependency("jetty", "6.1.26.hwx")},
      {"jetty-all-7.6.0.v20120127.jar", new Dependency("jetty", "7.6.0.v20120127")},
      {"listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar", new Dependency("listenablefuture", "9999.0")},
      {"json-20080701.jar", new Dependency("json", "20080701")},
      {"json4s-ast_2.10-3.2.10.jar", new Dependency("json4s", "3.2.10")},
      {"jython.jar", new Dependency("jython", "")},
      {"kafka-avro-serializer-2.0.1.jar", new Dependency("kafka-avro-serializer", "2.0.1")},
      {"kafka-clients-0.10.0-kafka-2.1.1.jar", new Dependency("kafka", "0.10.0-kafka-2.1.1")},
      {"kafka-clients-0.10.0.2.5.0.0-1245.jar", new Dependency("kafka", "0.10.0.2.5.0.0-1245")},
      {"kafka-clients-0.9.0.0-mapr-1707-beta.jar", new Dependency("kafka", "0.9.0.0-mapr-1707-beta")},
      {"kafka-clients-5.3.1-ccs.jar", new Dependency("kafka", "5.3.1")},
      {"kafka-schema-registry-client-3.0.1.jar", new Dependency("kafka-schema-registry-client", "3.0.1")},
      {"log4j-1.2-api-2.6.2.jar", new Dependency("log4j", "2.6.2")},
      {"mail.jar", new Dependency("mail", "")},
      {"mariadb-java-client-1.5.7.jar", new Dependency("mariadb-java-client", "1.5.7")},
      {"mapr-streams-6.0.0-mapr-beta.jar", new Dependency("mapr", "6.0.0-mapr-beta")},
      {"maprdb-cdc-6.0.0-mapr-beta.jar", new Dependency("mapr", "6.0.0-mapr-beta")},
      {"maprfs-5.2.0-mapr.jar", new Dependency("mapr", "5.2.0-mapr")},
      {"maprfs-6.0.0-mapr-beta.jar", new Dependency("mapr", "6.0.0-mapr-beta")},
      {"metrics-core-2.2.0.jar", new Dependency("metrics", "2.2.0")},
      {"mssql-jdbc-6.2.2.jre8.jar", new Dependency("mssql-jdbc", "6.2.2")},
      {"mysql-connector-java-5.1.40-bin.jar", new Dependency("mysql-connector-java", "5.1.40")},
      {"metrics-graphite-3.1.2.jar", new Dependency("metrics", "3.1.2")},
      {"netty-3.10.5.Final.jar", new Dependency("netty", "3.10.5")},
      {"netty-all-4.0.42.Final.jar", new Dependency("netty", "4.0.42")},
      {"netty-transport-native-epoll-4.1.16.Final-linux-x86_64.jar", new Dependency("netty", "4.1.16")},
      {"netty-tcnative-boringssl-static-2.0.3.Final.jar", new Dependency("netty-tcnative-boringssl-static", "2.0.3")},
      {"netty-tcnative-boringssl-static-1.1.33.Fork19.jar", new Dependency("netty-tcnative-boringssl-static", "1.1.33.Fork19")},
      {"nzjdbc3.jar", new Dependency("nzjdbc3", "")},
      {"ojdbc6.jar", new Dependency("ojdbc", "6")},
      {"ojdbc8.jar", new Dependency("ojdbc", "8")},
      {"org.apache.servicemix.bundles.cglib-3.2.4_1.jar", new Dependency("org.apache.servicemix.bundles.cglib", "3.2.4_1")},
      {"parquet-avro-1.8.1.jar", new Dependency("parquet", "1.8.1")},
      {"parquet-format-2.3.0-incubating.jar", new Dependency("parquet", "2.3.0-incubating")},
      {"pentaho-aggdesigner-algorithm-5.1.5-jhyde.jar", new Dependency("pentaho-aggdesigner-algorithm", "5.1.5-jhyde")},
      {"postgresql-9.2-1002.jdbc4.jar", new Dependency("postgresql", "9.2-1002")},
      {"protobuf-java-2.4.1-shaded.jar", new Dependency("protobuf-java", "2.4.1")},
      {"spark-core_2.10-1.6.0-cdh5.10.1.jar", new Dependency("spark", "1.6.0-cdh5.10.1")},
      {"spark-core_2.10-1.6.1-mapr-1607.jar", new Dependency("spark", "1.6.1-mapr-1607")},
      {"spark-core_2.11-2.1.0.cloudera1.jar", new Dependency("spark", "2.1.0.cloudera1")},
      {"spark-core_2.11-2.2.0-cdh6.0.0-beta1.jar", new Dependency("spark", "2.2.0-cdh6.0.0-beta1")},
      {"spark-streaming-kafka_2.10-1.6.1.2.4.2.0-258.jar", new Dependency("spark", "1.6.1.2.4.2.0-258")},
      {"spring-aop-4.3.8.RELEASE.jar", new Dependency("spring-aop", "4.3.8.RELEASE")},
      {"streamsets-datacollector-apache-kafka_0_10-lib-3.0.0.0.jar", new Dependency("streamsets", "3.0.0.0")},
      // This is specific of SDC build - from some reason we resolve "SNAPSHOT" into the timestamp
      {"streamsets-datacollector-spark-api-3.1.0.0-20171107.193324-1.jar", new Dependency("streamsets-datacollector-spark-api", "3.1.0.0-20171107.193324-1")},
      {"streamsets-datacollector-dataprotector-lib-1.3.0.jar", new Dependency("streamsets-datacollector-dataprotector", "1.3.0")},
      {"swagger-annotations-1.5.13.jar", new Dependency("swagger", "1.5.13")},
      {"swagger-core-1.5.13.jar", new Dependency("swagger", "1.5.13")},
      {"swagger-jaxrs-1.5.13.jar", new Dependency("swagger", "1.5.13")},
      {"sqljdbc4.jar", new Dependency("sqljdbc4", "")},
      {"tachyon-client-0.8.2.jar", new Dependency("tachyon", "0.8.2")},
      {"tdgssconfig.jar", new Dependency("tdgssconfig", "")},
      {"terajdbc4.jar", new Dependency("terajdbc4", "")},
      {"twill-api-0.6.0-incubating.jar", new Dependency("twill", "0.6.0-incubating")},
      {"uncommons-maths-1.2.2a.jar", new Dependency("uncommons-maths", "1.2.2a")},
      {"websocket-api-9.4.2.v20170220.jar", new Dependency("websocket", "9.4.2.v20170220")},
      {"xdb6.jar", new Dependency("xdb6", "")},
      {"xmlparserv2-11.1.1.2.0-patched.jar", new Dependency("xmlparserv2", "11.1.1.2.0-patched")},
      {"zookeeper-3.4.10.jar", new Dependency("zookeeper", "3.4.10")},
      {"zookeeper-3.4.5-cdh5.10.1.jar", new Dependency("zookeeper", "3.4.5-cdh5.10.1")},
      {"zookeeper-3.4.6.2.4.2.0-258-tests.jar", new Dependency("zookeeper", "3.4.6.2.4.2.0-258")},
      {"orc-core-1.5.2-nohive.jar", new Dependency("orc", "1.5.2-nohive")},
    });
  }

  private String jar;
  private Dependency expected;
  public DependencyParserExhaustiveTest(String jar, Dependency expected) {
    this.jar = jar;
    this.expected = expected;
  }

  @Test
  public void testJarNameParser() {
    String message = "For jar " + jar;
    Optional<Dependency> parsedOptional = DependencyParser.parseJarName(jar, jar);
    Assert.assertTrue(message, parsedOptional.isPresent());

    Dependency parsed = parsedOptional.get();
    Assert.assertNotNull(message, parsed);
    Assert.assertEquals(message, jar, parsed.getSourceName());
    Assert.assertEquals(message, expected.getName(), parsed.getName());
    Assert.assertEquals(message, expected.getVersion(), parsed.getVersion());
  }
}
