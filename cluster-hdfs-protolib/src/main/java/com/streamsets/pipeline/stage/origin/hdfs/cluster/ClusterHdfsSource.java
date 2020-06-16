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
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.cluster.Consumer;
import com.streamsets.pipeline.cluster.ControlChannel;
import com.streamsets.pipeline.cluster.DataChannel;
import com.streamsets.pipeline.cluster.Producer;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.impl.OffsetAndResult;
import com.streamsets.pipeline.impl.Pair;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.streamsets.pipeline.Utils.CLUSTER_HDFS_CONFIG_BEAN_PREFIX;

public class ClusterHdfsSource extends BaseSource implements OffsetCommitter, ErrorListener, ClusterSource {
  static final String TEXTINPUTFORMAT_RECORD_DELIMITER = "textinputformat.record.delimiter";

  private static final Logger LOG = LoggerFactory.getLogger(ClusterHdfsSource.class);
  private static final String DATA_FORMAT_CONFIG_BEAN_PREFIX = "clusterHDFSConfigBean.dataFormatConfig.";
  private static final int PREVIEW_SIZE = 100;
  private static final String HDFS_DIR_LOCATIONS = "hdfsDirLocations";
  private static final String HADOOP_CONF_DIR = "hdfsConfDir";
  private static final String HDFS_URI = "hdfsUri";
  private static final String CORE_SITE_XML = "core-site.xml";
  private static final String YARN_SITE_XML = "yarn-site.xml";
  private static final String HDFS_SITE_XML = "hdfs-site.xml";
  private static final String MAPRED_SITE_XML = "mapred-site.xml";

  private static final String CLUSTER_HDFS_SOURCE_CONFIG = "cluster-hdfs-source.properties";
  private static final String REQUIRE_HADOOP_CONF_KEY = "require.hadoop.conf";
  private static final String REQUIRE_URI_AUTHORITY_KEY = "require.uri.authority";

  private static final boolean REQUIRE_HADOOP_CONF;
  private static final boolean REQUIRE_URI_AUTHORITY;

  static {
    boolean hadoopConf = true;
    boolean uriAuthority = true;
    try (InputStream is = Thread.currentThread()
                                .getContextClassLoader()
                                .getResourceAsStream(CLUSTER_HDFS_SOURCE_CONFIG)) {
      if (is != null) {
        Properties properties = new Properties();
        properties.load(is);
        hadoopConf = Boolean.parseBoolean(properties.getProperty(REQUIRE_HADOOP_CONF_KEY, "true"));
        uriAuthority = Boolean.parseBoolean(properties.getProperty(REQUIRE_URI_AUTHORITY_KEY, "true"));
      }
    } catch (IOException ex) {
      //ignore
    }
    REQUIRE_HADOOP_CONF = hadoopConf;
    REQUIRE_URI_AUTHORITY = uriAuthority;
  }

  private final Producer producer;
  private final Consumer consumer;
  private final Map<String, Object> previewBuffer;
  private final CountDownLatch countDownLatch;
  private final ClusterHdfsConfigBean conf;

  private Configuration hadoopConf;
  private ErrorRecordHandler errorRecordHandler;
  private DataParserFactory parserFactory;
  private UserGroupInformation userUgi;
  private long recordsProduced;
  private boolean hasHeader;
  private String proxyUser;

  private final Set<String> visitedFiles;

  public ClusterHdfsSource(ClusterHdfsConfigBean conf) {
    ControlChannel controlChannel = new ControlChannel();
    DataChannel dataChannel = new DataChannel();
    producer = new Producer(controlChannel, dataChannel);
    consumer = new Consumer(controlChannel, dataChannel);
    this.recordsProduced = 0;
    this.previewBuffer = new LinkedHashMap<>();
    this.countDownLatch = new CountDownLatch(1);
    this.conf = conf;
    visitedFiles = new HashSet<>();
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    conf.dataFormatConfig.checkForInvalidAvroSchemaLookupMode(
        conf.dataFormat,
        "conf.dataFormatConfig",
        getContext(),
        issues
    );

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    validateDelimitedConfigIfNeeded(issues);

    getHadoopConfiguration(issues);

    try {
      String awsAccessKey = conf.awsAccessKey.get();
      if (!awsAccessKey.isEmpty()) {
        hadoopConf.set("fs.s3a.access.key", awsAccessKey);
      }
    } catch (StageException ex) {
      issues.add(getContext().createConfigIssue("S3", "awsAccessKey", ex.getErrorCode(), ex.getParams()));
    }
    try {
      String awsSecretKey = conf.awsSecretKey.get();
      if (!awsSecretKey.isEmpty()) {
        hadoopConf.set("fs.s3a.secret.key", awsSecretKey);
      }
    } catch (StageException ex) {
      issues.add(getContext().createConfigIssue("S3", "awsSecretKey", ex.getErrorCode(), ex.getParams()));
    }

    validateHadoopFS(issues);

    // This is for getting no of splits - no of executors
    hadoopConf.set(FileInputFormat.LIST_STATUS_NUM_THREADS, "5"); // Per Hive-on-Spark
    hadoopConf.set(FileInputFormat.SPLIT_MAXSIZE, String.valueOf(750000000)); // Per Hive-on-Spark
    for (Map.Entry<String, String> config : conf.hdfsConfigs.entrySet()) {
      hadoopConf.set(config.getKey(), config.getValue());
    }

    if (conf.dataFormat == DataFormat.TEXT && conf.dataFormatConfig.useCustomDelimiter) {
      hadoopConf.set(TEXTINPUTFORMAT_RECORD_DELIMITER, conf.dataFormatConfig.customDelimiter);
    }

    List<Path> hdfsDirPaths = validateAndGetHdfsDirPaths(issues);

    hadoopConf.set(FileInputFormat.INPUT_DIR, StringUtils.join(hdfsDirPaths, ","));
    hadoopConf.set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.toString(conf.recursive));

    // CsvHeader.IGNORE_HEADER must be overridden to CsvHeader.NO_HEADER prior to building the parser.
    // But it must be set back to the original value for the produce() method.
    CsvHeader originalCsvHeader = conf.dataFormatConfig.csvHeader;
    if (originalCsvHeader != null && originalCsvHeader == CsvHeader.IGNORE_HEADER) {
      conf.dataFormatConfig.csvHeader = CsvHeader.NO_HEADER;
    }
    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        Groups.HADOOP_FS.name(), DATA_FORMAT_CONFIG_BEAN_PREFIX,
        issues
    );
    conf.dataFormatConfig.csvHeader = originalCsvHeader;

    parserFactory = conf.dataFormatConfig.getParserFactory();

    hasHeader = conf.dataFormat == DataFormat.DELIMITED && CsvHeader.NO_HEADER != conf.dataFormatConfig.csvHeader;

    LOG.error("Issues: {}", issues.toArray());
    return issues;
  }

  @VisibleForTesting
  List<Path> validateAndGetHdfsDirPaths(List<ConfigIssue> issues) {
    List<Path> hdfsDirPaths = new ArrayList<>();

    // Don't proceed if there are already configuration issues.
    if (!issues.isEmpty()) {
      return hdfsDirPaths;
    }

    if (conf.hdfsDirLocations == null || conf.hdfsDirLocations.isEmpty()) {
      issues.add(
          getContext().createConfigIssue(
              Groups.HADOOP_FS.name(),
              CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HDFS_DIR_LOCATIONS,
              Errors.HADOOPFS_18
          )
      );
      return hdfsDirPaths;
    }


    for (String hdfsDirLocation : conf.hdfsDirLocations) {
      Path ph = new Path(hdfsDirLocation);
      try (FileSystem fs = getFileSystemForInitDestroy(ph)) {
        hdfsDirPaths.add(ph);

        if (!fs.exists(ph)) {
          issues.add(getContext().createConfigIssue(
              Groups.HADOOP_FS.name(),
              CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HDFS_DIR_LOCATIONS,
              Errors.HADOOPFS_10,
              hdfsDirLocation
          ));
          return hdfsDirPaths;
        }

        if (!fs.getFileStatus(ph).isDirectory()) {
          issues.add(getContext().createConfigIssue(
              Groups.HADOOP_FS.name(),
              CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HDFS_DIR_LOCATIONS,
              Errors.HADOOPFS_15,
              hdfsDirLocation
          ));
          return hdfsDirPaths;
        }

        checkForFiles(issues, hdfsDirLocation, fs, ph);
      } catch (IOException ioe) {
        LOG.warn("Error connecting to HDFS filesystem: " + ioe, ioe);
        issues.add(getContext().createConfigIssue(
            Groups.HADOOP_FS.name(),
            CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HDFS_DIR_LOCATIONS,
            Errors.HADOOPFS_11,
            hdfsDirLocation,
            ioe.toString(),
            ioe
        ));
      }
    }
    return hdfsDirPaths;
  }

  private void checkForFiles(List<ConfigIssue> issues, String hdfsDirLocation, FileSystem fs, Path ph) {
    try {
      FileStatus[] files = fs.listStatus(ph);
      if (files == null || files.length == 0) {
        issues.add(getContext().createConfigIssue(
            Groups.HADOOP_FS.name(),
            CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HDFS_DIR_LOCATIONS,
            Errors.HADOOPFS_17,
            hdfsDirLocation
        ));
      } else if (getContext().isPreview()) {
        readInPreview(fs, files, issues);
      }
    } catch (IOException | InterruptedException ex) {
      issues.add(getContext().createConfigIssue(
          Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HDFS_DIR_LOCATIONS,
          Errors.HADOOPFS_09,
          hdfsDirLocation,
          ex.toString(),
          ex
      ));
    }
  }

  @VisibleForTesting
  void readInPreview(final FileSystem fs, final FileStatus[] files, final List<ConfigIssue> issues) throws
      IOException,InterruptedException {
    // Setup for recursive visits to files in subdirectories
    List<FileStatus> queue = new LinkedList<>();
    Collections.addAll(queue, files);
    getUGI().doAs((PrivilegedExceptionAction<Void>) () -> {
      while(previewBuffer.size() < PREVIEW_SIZE && !queue.isEmpty()){
        FileStatus fileStatus = queue.remove(0);
        if (fileStatus.isFile()){
          readInPreview(fileStatus, issues);
        } else if (fileStatus.isDirectory()){
          Path ph = fs.makeQualified(new Path(fileStatus.getPath().toString()));
          FileStatus[] subs = fs.listStatus(ph);
          Collections.addAll(queue, subs);
        }
      }
      return null;
    });
  }

  @VisibleForTesting
  void readInPreview(FileStatus fileStatus, List<ConfigIssue> issues) {
    String path = fileStatus.getPath().toString();
    try {
      List<Map.Entry> buffer;
      if (conf.dataFormat == DataFormat.AVRO) {
        buffer = previewAvroBatch(fileStatus, PREVIEW_SIZE);
      } else {
        buffer = previewTextBatch(fileStatus, PREVIEW_SIZE);
      }
      for (int i = 0; i < buffer.size() && previewBuffer.size() < PREVIEW_SIZE; i++) {
        Map.Entry entry = buffer.get(i);
        previewBuffer.put(String.valueOf(entry.getKey()), entry.getValue());
      }
    } catch (IOException | InterruptedException ex) {
      LOG.warn(Utils.format(Errors.HADOOPFS_16.getMessage(), path, ex), ex);
      issues.add(
          getContext().createConfigIssue(
              Groups.HADOOP_FS.name(),
              CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HDFS_DIR_LOCATIONS,
              Errors.HADOOPFS_16,
              path,
              ex.toString()
          )
      );
    }
  }

  @VisibleForTesting
  List<Map.Entry> previewTextBatch(FileStatus fileStatus, int batchSize)
      throws IOException, InterruptedException {
    int previewCount = previewBuffer.size();
    TextInputFormat textInputFormat = new TextInputFormat();
    long fileLength = fileStatus.getLen();
    List<Map.Entry> batch = new ArrayList<>();
    Path filePath = fileStatus.getPath();
    // MR allows file length to be 0 for text data (not for avro)
    if (fileLength == 0) {
      LOG.info("File length is 0 for {}", filePath);
      return batch;
    }
    // Hadoop does unsafe casting from long to int, so split length should not be greater than int
    // max value
    long splitLength = (fileLength < Integer.MAX_VALUE) ? fileLength: Integer.MAX_VALUE;
    InputSplit fileSplit = new FileSplit(filePath, 0, splitLength, null);
    TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(hadoopConf,
        TaskAttemptID.forName("attempt_1439420318532_0011_m_000000_0"));

    try (RecordReader<LongWritable, Text> recordReader = textInputFormat.createRecordReader(fileSplit, taskAttemptContext)) {
      recordReader.initialize(fileSplit, taskAttemptContext);
      boolean hasNext = recordReader.nextKeyValue();

      while (hasNext && batch.size() < batchSize && previewCount < batchSize) {
        batch.add(new Pair(filePath.toUri().getPath() + "::" + recordReader.getCurrentKey(),
            String.valueOf(recordReader.getCurrentValue())));
        hasNext = recordReader.nextKeyValue(); // not like iterator.hasNext, actually advances
        previewCount++;
      }
    }
    return batch;
  }

  private List<Map.Entry> previewAvroBatch(FileStatus fileStatus, int batchSize) throws IOException {
    int previewCount = previewBuffer.size();
    Path filePath = fileStatus.getPath();
    SeekableInput input = new FsInput(filePath, hadoopConf);
    DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    List<Map.Entry> batch = new ArrayList<>();
    try (FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader)) {
      int count = 0;
      while (fileReader.hasNext() && batch.size() < batchSize && previewCount < batchSize) {
        GenericRecord datum = fileReader.next();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>
            (datum.getSchema()));
        try {
          dataFileWriter.create(datum.getSchema(), out);
          dataFileWriter.append(datum);
        } finally {
          dataFileWriter.close();
          out.close();
        }
        batch.add(new Pair(filePath.toUri().getPath() + "::" + count, out.toByteArray()));
        count++;
        previewCount++;
      }
    }
    return batch;
  }

  @VisibleForTesting
  Configuration getConfiguration() {
    return hadoopConf;
  }

  @VisibleForTesting
  protected void validateHadoopConfigFiles(File hadoopConfigDir, List<ConfigIssue> issues) {
    List<File> hadoopConfigFiles = ImmutableList.<File>builder()
        .add(new File(hadoopConfigDir, CORE_SITE_XML))
        .add(new File(hadoopConfigDir, HDFS_SITE_XML))
        .add(new File(hadoopConfigDir, YARN_SITE_XML))
        .add(new File(hadoopConfigDir, MAPRED_SITE_XML))
        .build();

    hadoopConfigFiles.forEach(file -> addHadoopConfigFile(file).ifPresent(issues::add));
  }

  private void validateDelimitedConfigIfNeeded(List<ConfigIssue> issues) {
    if (conf.dataFormat == DataFormat.DELIMITED && conf.dataFormatConfig.csvSkipStartLines != 0) {
      issues.add(
          getContext().createConfigIssue(
              Groups.HADOOP_FS.name(),
              CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "dataFormatConfig.csvSkipStartLines",
              Errors.HADOOPFS_32
          )
      );
    }
  }

  private Optional<ConfigIssue> addHadoopConfigFile(File file) {
    if (file.isFile()) {
      hadoopConf.addResource(new Path(file.getAbsolutePath()));
    } else if (file.exists()) {
      return Optional.of(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HADOOP_CONF_DIR,
          Errors.HADOOPFS_27,
          file.getAbsolutePath()));
    } else if (shouldHadoopConfigsExist()) {
      return Optional.of(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HADOOP_CONF_DIR,
          Errors.HADOOPFS_30,
          file.getName()
      ));
    }
    return Optional.empty();
  }

  protected boolean shouldHadoopConfigsExist() {
    return REQUIRE_HADOOP_CONF;
  }

  protected void getHadoopConfiguration(List<ConfigIssue> issues) {
    hadoopConf = new Configuration();

    // We will handle the file system close ourselves in destroy
    // See https://issues.streamsets.com/browse/SDC-4057
    hadoopConf.setBoolean("fs.automatic.close", false);

    if (conf.hdfsKerberos) {
      hadoopConf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
          UserGroupInformation.AuthenticationMethod.KERBEROS.name());
      try {
        hadoopConf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, "hdfs/_HOST@" + HadoopSecurityUtil.getDefaultRealm());
      } catch (Exception ex) {
        LOG.error(ex.toString(), ex);
        if (!conf.hdfsConfigs.containsKey(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)) {
          issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), null, Errors.HADOOPFS_28,
              ex.getMessage()));
        }
      }
    }
    if (!Strings.isNullOrEmpty(conf.hdfsConfDir)) {
      File hadoopConfigDir = new File(conf.hdfsConfDir);
      if (hadoopConfigDir.isAbsolute()) {
        // Do not allow absolute hadoop config directory in cluster mode
        issues.add(
            getContext().createConfigIssue(
                Groups.HADOOP_FS.name(),
                CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HADOOP_CONF_DIR,
                Errors.HADOOPFS_29,
                conf.hdfsConfDir,
                conf.hdfsConfDir,
                getContext().getResourcesDirectory()
            )
        );
      } else {
        // Verify that user is not specifying directory outside of resources dir by doing ../../ - as that
        // will not work in cluster mode.
        try {
          hadoopConfigDir = new File(getContext().getResourcesDirectory(), conf.hdfsConfDir).getCanonicalFile();

          if(!hadoopConfigDir.getPath().startsWith(new File(getContext().getResourcesDirectory()).getCanonicalPath())) {
            issues.add(
                getContext().createConfigIssue(
                    Groups.HADOOP_FS.name(),
                    CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HADOOP_CONF_DIR,
                    Errors.HADOOPFS_29,
                    conf.hdfsConfDir,
                    hadoopConfigDir.getAbsolutePath(),
                    getContext().getResourcesDirectory()
                )
            );
          }
        } catch (IOException e) {
          LOG.error("Can't resolve configuration directory", e);
          issues.add(
              getContext().createConfigIssue(
                  Groups.HADOOP_FS.name(),
                  CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HADOOP_CONF_DIR,
                  Errors.HADOOPFS_26,
                  conf.hdfsConfDir
              )
          );
        }
      }

      if (!hadoopConfigDir.exists()) {
        issues.add(
            getContext().createConfigIssue(
                Groups.HADOOP_FS.name(),
                CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HADOOP_CONF_DIR,
                Errors.HADOOPFS_25,
                hadoopConfigDir.getPath()
            )
        );
      } else if (!hadoopConfigDir.isDirectory()) {
        issues.add(
            getContext().createConfigIssue(
                Groups.HADOOP_FS.name(),
                CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HADOOP_CONF_DIR,
                Errors.HADOOPFS_26,
                hadoopConfigDir.getPath()
            )
        );
      } else {
        validateHadoopConfigFiles(hadoopConfigDir, issues);
      }
    } else if (shouldHadoopConfigsExist()) {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HADOOP_CONF_DIR,
          Errors.HADOOPFS_31
      ));
    }
    for (Map.Entry<String, String> config : conf.hdfsConfigs.entrySet()) {
      hadoopConf.set(config.getKey(), config.getValue());
    }
  }

  @VisibleForTesting
  void validateHadoopFS(List<ConfigIssue> issues) {
    boolean validHadoopFsUri;
    String hdfsUriInConf;
    if (!Strings.isNullOrEmpty(conf.hdfsUri)) {
      hadoopConf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, conf.hdfsUri);
    } else {
      hdfsUriInConf = hadoopConf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
      if (hdfsUriInConf == null) {
        issues.add(
            getContext().createConfigIssue(
                Groups.HADOOP_FS.name(),
                CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HDFS_URI,
                Errors.HADOOPFS_19
            )
        );
        return;
      } else {
        conf.hdfsUri = hdfsUriInConf;
      }
    }
    validHadoopFsUri = validateHadoopFsURI(issues);
    StringBuilder logMessage = new StringBuilder();
    try {
      UserGroupInformation loginUgi = HadoopSecurityUtil.getLoginUser(hadoopConf);
      userUgi = HadoopSecurityUtil.getProxyUser(
          conf.hdfsUser,
          getContext(),
          loginUgi,
          issues,
          Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsUser"
      );
      if (userUgi != loginUgi) {
        proxyUser = userUgi.getUserName();
        LOG.debug("Proxy user submitting cluster batch job is {}", proxyUser);
      }
      if (conf.hdfsKerberos) {
        logMessage.append("Using Kerberos");
        if (loginUgi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.HADOOP_FS.name(),
                  CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsKerberos",
                  Errors.HADOOPFS_00,
                  loginUgi.getAuthenticationMethod(),
                  UserGroupInformation.AuthenticationMethod.KERBEROS
              )
          );
        }
      } else {
        logMessage.append("Using Simple");
        hadoopConf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
            UserGroupInformation.AuthenticationMethod.SIMPLE.name());
      }
      if (validHadoopFsUri) {
        getUGI().doAs((PrivilegedExceptionAction<Void>) () -> {
          try (FileSystem fs = getFileSystemForInitDestroy(null)) { // NOSONAR
            // to trigger fs close
          }
          return null;
        });
      }
    } catch (Exception ex) {
      LOG.info("Error connecting to FileSystem: " + ex, ex);
      issues.add(
          getContext().createConfigIssue(
              Groups.HADOOP_FS.name(),
              null,
              Errors.HADOOPFS_11,
              conf.hdfsUri,
              String.valueOf(ex),
              ex
          )
      );
    }
    LOG.info("Authentication Config: {}", logMessage);
  }

  @VisibleForTesting
  protected boolean validateHadoopFsURI(List<ConfigIssue> issues) {
    boolean validHadoopFsUri = true;
    if (conf.hdfsUri.contains("://")) {
      try {
        URI uri = new URI(conf.hdfsUri);
        if (isURIAuthorityRequired() && uri.getAuthority() == null) {
          issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
              CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HDFS_URI,
              Errors.HADOOPFS_13,
              conf.hdfsUri
          ));
          validHadoopFsUri = false;
        }
      } catch (Exception ex) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
            CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HDFS_URI,
            Errors.HADOOPFS_22,
            conf.hdfsUri,
            ex.getMessage(),
            ex
        ));
        validHadoopFsUri = false;
      }
    } else {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + HDFS_URI,
          Errors.HADOOPFS_02,
          conf.hdfsUri
      ));
      validHadoopFsUri = false;
    }
    return validHadoopFsUri;
  }

  protected boolean isURIAuthorityRequired() {
    return REQUIRE_URI_AUTHORITY;
  }

  @VisibleForTesting
  FileSystem getFileSystemForInitDestroy(Path path) throws IOException {
    try {
      return getUGI().doAs((PrivilegedExceptionAction<FileSystem>) () -> (path != null)? FileSystem.newInstance(path.toUri(), hadoopConf) : FileSystem.newInstance(new URI(conf.hdfsUri), hadoopConf));
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(ex); // NOSONAR
    }
  }

  @VisibleForTesting
  UserGroupInformation getUGI() {
    return userUgi;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    OffsetAndResult<Map.Entry> offsetAndResult;
    if (getContext().isPreview()) {
      // we support text and csv today
      List<Map.Entry> records = new ArrayList<>();
      int count = 0;
      Iterator<Map.Entry<String, Object>> keys = previewBuffer.entrySet().iterator();
      while (count < maxBatchSize && count < previewBuffer.size() && keys.hasNext()) {
        Map.Entry<String, Object> entry =  keys.next();
        String[] keyParts = entry.getKey().split("::");
        boolean isFirst = keyParts.length > 1 && "0".equals(keyParts[1]);
        if (hasHeader && count == 0 && isFirst) {
          // add header
          if (CsvHeader.WITH_HEADER == conf.dataFormatConfig.csvHeader) {
            records.add(new Pair(entry.getValue(), null));
          }
        } else {
          records.add(new Pair(entry.getKey(), entry.getValue()));
          count++;
        }

      }
      offsetAndResult = new OffsetAndResult<>(recordsProduced, records);
    } else {
      offsetAndResult = consumer.take();
    }
    if (offsetAndResult == null) {
      LOG.info("Received null batch, returning null");
      return null;
    }
    String messageId = null;
    int count = 0;
    String header = null;
    for (Map.Entry message : offsetAndResult.getResult()) {
      count++;
      messageId = String.valueOf(message.getKey());
      List<Record> listRecords = Collections.emptyList();
      if (conf.dataFormat == DataFormat.TEXT || conf.dataFormat == DataFormat.AVRO) {
        listRecords = processMessage(messageId, message.getValue());
      } else if (conf.dataFormat == DataFormat.DELIMITED) {
        switch (conf.dataFormatConfig.csvHeader) {
          case IGNORE_HEADER:
            // ignore header by skipping this header string
            // [1] - startOffset - [2] - contextKey
            String[] offsetContextSplit = messageId.split("::");
            if (offsetContextSplit.length > 1 && "0".equals(offsetContextSplit[1])) {
              break;
            }
            listRecords = processMessage(messageId, message.getValue());
            break;
          case NO_HEADER:
            listRecords = processMessage(messageId, message.getValue());
            break;
          case WITH_HEADER:
            if (header == null) {
              header = messageId;
              LOG.debug("Header is: {}", header);
              Utils.checkState(message.getValue() == null, Utils.formatL("Message value for header record should be null, was: '{}'", message.getValue()));
            } else {
              listRecords = processMessage(messageId, header + "\n" + message.getValue());
            }
            break;
          default:
            String msg = Utils.format("Unrecognized header: '{}'", conf.dataFormatConfig.csvHeader);
            LOG.warn(msg);
            throw new IllegalStateException(msg);
        }
      } else {
        throw new IllegalStateException(Utils.format("Unrecognized data format: '{}'", conf.dataFormat));
      }
      listRecords.forEach(batchMaker::addRecord);

      if (!listRecords.isEmpty()) {
        String fileName = listRecords.get(0).getHeader().getAttribute(HeaderAttributeConstants.FILE);
        if (!visitedFiles.contains(fileName)) {
          visitedFiles.add(fileName);
          sendLineageEvent(fileName);
        }
      }
    }
    if (count == 0) {
      LOG.info("Received no records, returning null");
      return null;
    }
    return Utils.checkNotNull(messageId, "Log error, message ID cannot be null at this point.");
  }

  private List<Record> processMessage(String messageId, Object message) throws StageException {
    List<Record> records = new ArrayList<>();
    if (conf.dataFormat == DataFormat.AVRO) {
      try (DataParser parser = parserFactory.getParser(messageId, (byte[]) message)) {
        Record record = parser.parse();
        if (record != null) {
          setHeaders(record);
          records.add(record);
        }
      } catch (IOException | DataParserException ex) {
        LOG.debug("Got exception: '{}'", ex, ex);
        errorRecordHandler.onError(Errors.HADOOPFS_08, messageId, ex.toString(), ex);
      }
    } else {
      try (DataParser parser = parserFactory.getParser(messageId, String.valueOf(message))) {
        Record record = null;
        do {
          try { // NOSONAR
            record = parser.parse();
          } catch (RecoverableDataParserException e) {
            LOG.warn(e.toString(), e);
            errorRecordHandler.onError(
                new OnRecordErrorException(
                    e.getUnparsedRecord(),
                    e.getErrorCode(),
                    e.getParams())
            );
            // Go to next record
            continue;
          }
          if (record != null) {
            setHeaders(record);
            records.add(record);
          }
        } while(record != null);
      } catch (IOException | DataParserException ex) {
        LOG.debug("Got exception: '{}'", ex, ex);
        errorRecordHandler.onError(Errors.HADOOPFS_08, messageId, ex.toString(), ex);
      }
    }

    if (conf.produceSingleRecordPerMessage) {
      List<Field> list = new ArrayList<>();
      records.forEach(record -> list.add(record.get()));
      if(!list.isEmpty()) {
        Record record = records.get(0);
        record.set(Field.create(Field.Type.LIST, list));
        records.clear();
        records.add(record);
      }
    }
    return records;
  }

  private void setHeaders(Record record) {
    String[] source = record.getHeader().getSourceId().split("::");
    record.getHeader().setAttribute(HeaderAttributeConstants.FILE, source[0]);
    record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, source[1]);
  }

  private void sendLineageEvent(String fileName) {
    LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_READ);
    event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, fileName);
    event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.HDFS.name());
    event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, fileName);
    getContext().publishLineageEvent(event);
  }

  @Override
  public Object put(List<Map.Entry> batch) throws InterruptedException {
    Object expectedOffset = producer.put(new OffsetAndResult<>(recordsProduced, batch));
    recordsProduced += batch.size();
    return expectedOffset;
  }

  @Override
  public void completeBatch() throws InterruptedException {
    producer.waitForCommit();
  }

  @Override
  public long getRecordsProduced() {
    return recordsProduced;
  }

  @Override
  public boolean inErrorState() {
    return producer.inErrorState() || consumer.inErrorState();
  }

  @Override
  public void errorNotification(Throwable throwable) {
    consumer.error(throwable);
  }

  @Override
  public void commit(String offset) throws StageException {
    consumer.commit(offset);
  }

  @Override
  public void destroy() {
    producer.complete();
    super.destroy();
  }

  @Override
  public void shutdown() {
    producer.complete();
    try {
      boolean isDone = countDownLatch.await(5, TimeUnit.MINUTES);
      if (!isDone) {
        LOG.warn("Pipeline is still in active state: {} after 5 minutes");
      } else {
        LOG.info("Destroy() on stages is complete");
      }
    } catch (InterruptedException e) {
      LOG.warn("Thread interrupted while waiting on receiving the done flag" + e, e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public int getParallelism() {
    return 1; // not used as MR calculates splits
  }

  @Override
  public String getName() {
    return "hdfs";
  }

  @Override
  public boolean isInBatchMode() {
    // for now directory source requires batch mode
    return true;
  }

  @Override
  public Map<String, String> getConfigsToShip() {
    Map<String, String> configsToShip = new HashMap<>();
    for (Map.Entry<String, String> entry : hadoopConf) {
      // hadoopConf.get() is required since entry.getValue()
      // does not have variables expanded
      configsToShip.put(entry.getKey(), hadoopConf.get(entry.getKey()));
    }
    if (proxyUser != null) {
      configsToShip.put(ClusterModeConstants.HADOOP_PROXY_USER, proxyUser);
    }
    return configsToShip;
  }

  @Override
  public void postDestroy() {
    countDownLatch.countDown();
  }
}
