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
package com.streamsets.pipeline.stage.destination.cassandra;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.dse.auth.DseGSSAPIAuthProvider;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Cassandra Destination for StreamSets Data Collector
 *
 * Some basic ground rules for the Cassandra Java Driver:
 * - Use one cluster instance per (physical) cluster (per application lifetime).
 * - Use at most one session instance per keyspace, or use a single Session and
 *   explicitly specify the keyspace in your queries.
 * - If you execute a statement more than once, consider using a prepared statement.
 * - You can reduce the number of network round trips and also have atomic operations by using batches.
 *
 */
public class CassandraTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraTarget.class);
  private static final String CONTACT_NODES_LABEL = "contactPoints";
  private static final List<TypeCodec<?>> SDC_CODECS = ImmutableList.of(
      new TimeUUIDAsStringCodec(),
      new UUIDAsStringCodec(),
      new LocalDateAsDateCodec()
  );

  private final CassandraTargetConfig conf;
  private List<InetAddress> contactPoints;

  private Cluster cluster;
  private Session session;

  private SortedMap<String, String> columnMappings;
  private LoadingCache<SortedSet<String>, PreparedStatement> statementCache;
  private ErrorRecordHandler errorRecordHandler;

  public CassandraTarget(CassandraTargetConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    Target.Context context = getContext();

    // Verify that we can load DSE classes if DSE auth modes are selected
    try {
      getAuthProvider();
    } catch (NoClassDefFoundError | StageException e) {
      LOG.error(Errors.CASSANDRA_10.getMessage(), conf.authProviderOption, e);
      issues.add(context.createConfigIssue(
          "CASSANDRA",
          "authProviderOption",
          Errors.CASSANDRA_10,
          conf.authProviderOption
      ));
      return issues;
    }

    if (conf.contactPoints.isEmpty()) {
      issues.add(context.createConfigIssue(Groups.CASSANDRA.name(), CONTACT_NODES_LABEL, Errors.CASSANDRA_00));
    }

    if (conf.contactPoints.stream().anyMatch(String::isEmpty)) {
      issues.add(context.createConfigIssue(Groups.CASSANDRA.name(), CONTACT_NODES_LABEL, Errors.CASSANDRA_01));
    }

    contactPoints = new ArrayList<>(conf.contactPoints.size());
    for (String address : conf.contactPoints) {
      try {
        contactPoints.add(InetAddress.getByName(address));
      } catch (UnknownHostException e) {
        LOG.error(Errors.CASSANDRA_04.getMessage(), address, e);
        issues.add(context.createConfigIssue(Groups.CASSANDRA.name(), CONTACT_NODES_LABEL, Errors.CASSANDRA_04, address));
      }
    }

    if (contactPoints.isEmpty()) {
      issues.add(context.createConfigIssue(Groups.CASSANDRA.name(), CONTACT_NODES_LABEL, Errors.CASSANDRA_00));
    }

    // Need to init TLS config before we touch Cassandra
    if (issues.isEmpty() && conf.tlsConfig.isEnabled()) {
      // this configuration has no separate "tlsEnabled" field on the bean level, so need to do it this way
      conf.tlsConfig.init(
          getContext(),
          Groups.TLS.name(),
          "conf.tlsConfig.",
          issues
      );
    }

    if (issues.isEmpty()) {
      if (!conf.qualifiedTableName.contains(".")) {
        issues.add(context.createConfigIssue(Groups.CASSANDRA.name(), "qualifiedTableName", Errors.CASSANDRA_02));
      } else {
        if (checkCassandraReachable(issues)) {
          try {
            List<String> invalidColumns = checkColumnMappings();
            if (!invalidColumns.isEmpty()) {
              issues.add(
                  context.createConfigIssue(
                  Groups.CASSANDRA.name(),
                  "columnNames",
                  Errors.CASSANDRA_08,
                  Joiner.on(", ").join(invalidColumns)
              ));
            }
          } catch (StageException e) {
            issues.add(
                context.createConfigIssue(
                Groups.CASSANDRA.name(),
                "columnNames",
                Errors.CASSANDRA_03,
                e.toString()
            ));
          }

        }
      }
    }

    if (issues.isEmpty()) {

      try {
        cluster = getCluster();
        session = cluster.connect();

        statementCache = CacheBuilder.newBuilder()
            // No expiration as prepared statements are good for the entire session.
            .build(
                new CacheLoader<SortedSet<String>, PreparedStatement>() {
                  @Override
                  public PreparedStatement load(@NotNull SortedSet<String> columns) {
                    // The INSERT query we're going to perform (parameterized).
                    SortedSet<String> statementColumns = new TreeSet<>();
                    for (String fieldPath : columnMappings.keySet()) {
                      final String fieldName = fieldPath.replaceAll("/", "");
                      if (columns.contains(fieldName)) {
                        statementColumns.add(fieldName);
                      }
                    }
                    final String query = String.format(
                        "INSERT INTO %s (%s) VALUES (%s);",
                        conf.qualifiedTableName,
                        Joiner.on(", ").join(statementColumns),
                        Joiner.on(", ").join(Collections.nCopies(statementColumns.size(), "?"))
                    );
                    LOG.trace("Prepared Query: {}", query);
                    return session.prepare(query);
                  }
                }
            );
      } catch (NoHostAvailableException | AuthenticationException | IllegalStateException | StageException e) {
        LOG.error(Errors.CASSANDRA_03.getMessage(), e.toString(), e);
        issues.add(context.createConfigIssue(null, null, Errors.CASSANDRA_03, e.toString()));
      }
    }

    return issues;
  }

  @Override
  public void destroy() {
    IOUtils.closeQuietly(session);
    IOUtils.closeQuietly(cluster);
    super.destroy();
  }

  private List<String> checkColumnMappings() throws StageException {
    List<String> invalidColumnMappings = new ArrayList<>();

    columnMappings = new TreeMap<>();
    for (CassandraFieldMappingConfig column : conf.columnNames) {
      columnMappings.put(column.columnName, column.field);
    }

    final String[] tableNameParts = conf.qualifiedTableName.split("\\.");
    final String keyspace = tableNameParts[0];
    final String table = tableNameParts[1];

    try (Cluster validationCluster = getCluster()) {
      final KeyspaceMetadata keyspaceMetadata = validationCluster.getMetadata().getKeyspace(keyspace);
      final TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
      final List<String> columns = Lists.transform(
          tableMetadata.getColumns(),
          new Function<ColumnMetadata, String>() {
            @Nullable
            @Override
            public String apply(ColumnMetadata columnMetadata) {
              return columnMetadata.getName();
            }
          }
      );

      invalidColumnMappings.addAll(columnMappings.keySet()
          .stream()
          .filter(columnName -> !columns.contains(columnName))
          .collect(Collectors.toList())
      );
    }

    return invalidColumnMappings;
  }

  private boolean checkCassandraReachable(List<ConfigIssue> issues) {
    boolean isReachable = true;
    try (Cluster validationCluster = getCluster()) {
      Session validationSession = validationCluster.connect();
      validationSession.close();
    } catch (NoHostAvailableException | AuthenticationException | IllegalStateException | StageException e) {
      isReachable = false;
      Target.Context context = getContext();
      LOG.error(Errors.CASSANDRA_05.getMessage(), e.toString(), e);
      issues.add(
          context.createConfigIssue(
          Groups.CASSANDRA.name(),
          CONTACT_NODES_LABEL,
          Errors.CASSANDRA_05, e.toString()
          )
      );
    }
    return isReachable;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(Batch batch) throws StageException {
    // The batch holding the current batch to INSERT.
    BatchStatement batchedStatement = new BatchStatement(conf.batchType);

    Iterator<Record> records = batch.getRecords();

    while (records.hasNext()) {
      final Record record = records.next();

      BoundStatement boundStmt = recordToBoundStatement(record);

      // if the bound statement is null here, it's probably because the record was an error record and should be
      // dropped or skipped. don't add it to batch statement.
      if (boundStmt != null) {
        // if this batch is currently at the max batch size, then let's execute it and make a new batch before adding
        // this latest statement to it.
        if (batchedStatement.size() == conf.maxBatchSize) {
          session.execute(batchedStatement);
          batchedStatement = new BatchStatement();
        }
        batchedStatement.add(boundStmt);
      }
    }

    // if there are any unexecuted statements, execute them now
    if (batchedStatement.size() > 0) {
      session.execute(batchedStatement);
    }
  }

  /**
   * Convert a Record into a fully-bound statement.
   */
  @SuppressWarnings("unchecked")
  private BoundStatement recordToBoundStatement(Record record) throws StageException {
    ImmutableList.Builder<Object> values = new ImmutableList.Builder<>();
    SortedSet<String> columnsPresent = Sets.newTreeSet(columnMappings.keySet());
    for (Map.Entry<String, String> mapping : columnMappings.entrySet()) {
      String columnName = mapping.getKey();
      String fieldPath = mapping.getValue();

      // If we're missing fields, skip them.
      // If a field is present, but null, also remove it from columnsPresent since we can't write nulls.
      if (!record.has(fieldPath) || record.get(fieldPath).getValue() == null) {
        columnsPresent.remove(columnName);
        continue;
      }

      final Object value = record.get(fieldPath).getValue();
      // Special cases for handling SDC Lists and Maps,
      // basically unpacking them into raw types.
      if (value instanceof List) {
        List<Object> unpackedList = new ArrayList<>();
        for (Field item : (List<Field>) value) {
          unpackedList.add(item.getValue());
        }
        values.add(unpackedList);
      } else if (value instanceof Map) {
        Map<Object, Object> unpackedMap = new HashMap<>();
        for (Map.Entry<String, Field> entry : ((Map<String, Field>) value).entrySet()) {
          unpackedMap.put(entry.getKey(), entry.getValue().getValue());
        }
        values.add(unpackedMap);
      } else {
        values.add(value);
      }
    }


    PreparedStatement stmt = statementCache.getUnchecked(columnsPresent);
    // .toArray required to pass in a list to a varargs method.
    Object[] valuesArray = values.build().toArray();
    BoundStatement boundStmt = null;
    try {
      boundStmt = stmt.bind(valuesArray);
    } catch (CodecNotFoundException | InvalidTypeException | NullPointerException e) {
      // NPE can occur if one of the values is a collection type with a null value inside it. Thus, it's a record
      // error. Note that this runs the risk of mistakenly treating a bug as a record error.
      // CodecNotFound is caused when there is no type conversion definition available from the provided type
      // to the target type.
      errorRecordHandler.onError(
          new OnRecordErrorException(
              record,
              Errors.CASSANDRA_06,
              record.getHeader().getSourceId(),
              e.toString(),
              e
          )
      );
    }
    return boundStmt;
  }

  private Cluster getCluster() throws StageException {
    RemoteEndpointAwareJdkSSLOptions sslOptions = null;

    if (conf.tlsConfig.isEnabled()) {
      // Not certain why we need the downcast here, but we do
      sslOptions = (RemoteEndpointAwareJdkSSLOptions)RemoteEndpointAwareJdkSSLOptions.builder()
          .withSSLContext(conf.tlsConfig.getSslContext())
          .build();
    }

    return Cluster.builder()
        .addContactPoints(contactPoints)
        .withSSL(sslOptions)
        // If authentication is disabled on the C* cluster, this method has no effect.
        .withAuthProvider(getAuthProvider())
        .withProtocolVersion(conf.protocolVersion)
        .withPort(conf.port)
        .withCodecRegistry(new CodecRegistry().register(SDC_CODECS))
        .build();
  }

  private AuthProvider getAuthProvider() throws StageException {
    switch (conf.authProviderOption) {
      case NONE:
        return AuthProvider.NONE;
      case PLAINTEXT:
        return new PlainTextAuthProvider(conf.username.get(), conf.password.get());
      case DSE_PLAINTEXT:
        return new DsePlainTextAuthProvider(conf.username.get(), conf.password.get());
      case KERBEROS:
        AccessControlContext accessContext = AccessController.getContext();
        Subject subject = Subject.getSubject(accessContext);
        return DseGSSAPIAuthProvider.builder().withSubject(subject).build();
      default:
        throw new IllegalArgumentException("Unrecognized AuthProvider: " + conf.authProviderOption);
    }
  }
}
