/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.streamsets.datacollector.security.HadoopSecurityUtil;
import com.streamsets.pipeline.cluster.Consumer;
import com.streamsets.pipeline.cluster.ControlChannel;
import com.streamsets.pipeline.cluster.DataChannel;
import com.streamsets.pipeline.cluster.Producer;
import com.streamsets.pipeline.impl.Pair;

import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.impl.OffsetAndResult;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;

import static com.streamsets.pipeline.Utils.CLUSTER_HDFS_CONFIG_BEAN_PREFIX;

public class ClusterHdfsSource extends BaseSource implements OffsetCommitter, ErrorListener, ClusterSource {

  public static final String DATA_FROMAT_CONFIG_BEAN_PREFIX = "clusterHDFSConfigBean.dataFormatConfig.";
  public static final String TEXTINPUTFORMAT_RECORD_DELIMITER = "textinputformat.record.delimiter";

  private static final Logger LOG = LoggerFactory.getLogger(ClusterHdfsSource.class);
  private static final int PREVIEW_SIZE = 100;

  private Configuration hadoopConf;
  private final ControlChannel controlChannel;
  private final DataChannel dataChannel;
  private final Producer producer;
  private final Consumer consumer;
  private ErrorRecordHandler errorRecordHandler;
  private DataParserFactory parserFactory;
  private UserGroupInformation loginUgi;
  private long recordsProduced;
  private final Map<String, Object> previewBuffer;
  private final CountDownLatch countDownLatch;
  private final ClusterHdfsConfigBean conf;
  private static final String CORE_SITE_XML = "core-site.xml";
  private static final String YARN_SITE_XML = "yarn-site.xml";
  private static final String HDFS_SITE_XML = "hdfs-site.xml";
  private static final String MAPRED_SITE_XML = "mapred-site.xml";


  public ClusterHdfsSource(ClusterHdfsConfigBean conf) {
    controlChannel = new ControlChannel();
    dataChannel = new DataChannel();
    producer = new Producer(controlChannel, dataChannel);
    consumer = new Consumer(controlChannel, dataChannel);
    this.recordsProduced = 0;
    this.previewBuffer = new LinkedHashMap<>();
    this.countDownLatch = new CountDownLatch(1);
    this.conf = conf;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    hadoopConf = getHadoopConfiguration(issues);

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
        Groups.HADOOP_FS.name(),
        DATA_FROMAT_CONFIG_BEAN_PREFIX,
        issues
    );
    conf.dataFormatConfig.csvHeader = originalCsvHeader;

    parserFactory = conf.dataFormatConfig.getParserFactory();

    LOG.info("Issues: " + issues);
    return issues;
  }

  @VisibleForTesting
  List<Path> validateAndGetHdfsDirPaths(List<ConfigIssue> issues) {
    List<Path> hdfsDirPaths = new ArrayList<>();
    if (conf.hdfsDirLocations == null || conf.hdfsDirLocations.isEmpty()) {
      issues.add(
          getContext().createConfigIssue(
              Groups.HADOOP_FS.name(),
              CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsDirLocations",
              Errors.HADOOPFS_18
          )
      );
    } else if (issues.isEmpty()) {
      for (String hdfsDirLocation : conf.hdfsDirLocations) {
        FileSystem fs = null;
        try {
          fs = getFileSystemForInitDestroy();
          Path ph = fs.makeQualified(new Path(hdfsDirLocation));
          hdfsDirPaths.add(ph);
          if (!fs.exists(ph)) {
            issues.add(
                getContext().createConfigIssue(
                    Groups.HADOOP_FS.name(),
                    CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsDirLocations",
                    Errors.HADOOPFS_10,
                    hdfsDirLocation
                )
            );
          } else if (!fs.getFileStatus(ph).isDirectory()) {
            issues.add(
                getContext().createConfigIssue(
                    Groups.HADOOP_FS.name(),
                    CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsDirLocations",
                    Errors.HADOOPFS_15,
                    hdfsDirLocation
                )
            );
          } else {
            try {
              FileStatus[] files = fs.listStatus(ph);
              if (files == null || files.length == 0) {
                issues.add(
                    getContext().createConfigIssue(
                        Groups.HADOOP_FS.name(),
                        CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsDirLocations",
                        Errors.HADOOPFS_16,
                        hdfsDirLocation
                    )
                );
              } else if (getContext().isPreview() && previewBuffer.size() < PREVIEW_SIZE) {
                for (FileStatus fileStatus : files) {
                  if (fileStatus.isFile()) {
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
                        previewBuffer.put(String.valueOf(entry.getKey()),
                          entry.getValue() == null ? null : entry.getValue());
                      }
                    } catch (IOException | InterruptedException ex) {
                      String msg = "Error opening " + path + ": " + ex;
                      LOG.info(msg, ex);
                      issues.add(
                          getContext().createConfigIssue(
                              Groups.HADOOP_FS.name(),
                              CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsDirLocations",
                              Errors.HADOOPFS_16,
                              fileStatus.getPath()
                          )
                      );
                    }
                  }
                }
              }
            } catch (IOException ex) {
              issues.add(
                  getContext().createConfigIssue(
                      Groups.HADOOP_FS.name(),
                      CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsDirLocations",
                      Errors.HADOOPFS_09,
                      hdfsDirLocation,
                      ex.toString(),
                      ex
                  )
              );
            }
          }
        } catch (IOException ioe) {
          LOG.warn("Error connecting to HDFS filesystem: " + ioe, ioe);
          issues.add(
              getContext().createConfigIssue(
                  Groups.HADOOP_FS.name(),
                  CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsDirLocations",
                  Errors.HADOOPFS_11,
                  hdfsDirLocation,
                  ioe.toString(),
                  ioe
              )
          );
        } finally {
          if (fs != null) {
            try {
              fs.close();
            } catch (IOException e) {
              LOG.info("Error closing FileSystem: ", e);
            }
          }
        }
      }
    }
    return hdfsDirPaths;
  }

  @VisibleForTesting
  List<Map.Entry> previewTextBatch(FileStatus fileStatus, int batchSize)
    throws IOException, InterruptedException {
    TextInputFormat textInputFormat = new TextInputFormat();
    long fileLength = fileStatus.getLen();
    List<Map.Entry> batch = new ArrayList<>();
    Path filePath = fileStatus.getPath();
    // MR allows file length to be 0 for text data (not for avro)
    if (fileLength == 0) {
      LOG.info("File length is 0 for " + filePath);
      return batch;
    }
    // Hadoop does unsafe casting from long to int, so split length should not be greater than int
    // max value
    long splitLength = (fileLength < Integer.MAX_VALUE) ? fileLength: Integer.MAX_VALUE;
    InputSplit fileSplit = new FileSplit(filePath, 0, splitLength, null);
    TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(hadoopConf,
      TaskAttemptID.forName("attempt_1439420318532_0011_m_000000_0"));
    RecordReader<LongWritable, Text> recordReader = textInputFormat.createRecordReader(fileSplit, taskAttemptContext);
    recordReader.initialize(fileSplit, taskAttemptContext);
    boolean hasNext = recordReader.nextKeyValue();

    while (hasNext && batch.size() < batchSize) {
      batch.add(new Pair(filePath.toUri().getPath() + "::" + recordReader.getCurrentKey(),
        String.valueOf(recordReader.getCurrentValue())));
      hasNext = recordReader.nextKeyValue(); // not like iterator.hasNext, actually advances
    }
    return batch;
  }

  private List<Map.Entry> previewAvroBatch(FileStatus fileStatus, int batchSize) throws IOException, InterruptedException {
    Path filePath = fileStatus.getPath();
    SeekableInput input = new FsInput(filePath, hadoopConf);
    DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);
    List<Map.Entry> batch = new ArrayList<>();
    int count = 0;
    while (fileReader.hasNext() && batch.size() < batchSize) {
      GenericRecord datum = fileReader.next();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      DataFileWriter<GenericRecord> dataFileWriter =
        new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(datum.getSchema()));
      dataFileWriter.create(datum.getSchema(), out);
      dataFileWriter.append(datum);
      dataFileWriter.close();
      out.close();
      batch.add(new Pair(filePath.toUri().getPath() + "::" + count, out.toByteArray()));
      count++;
    }
    return batch;
  }

  @VisibleForTesting
  Configuration getConfiguration() {
    return hadoopConf;
  }

  @VisibleForTesting
  protected void validateHadoopConfigFiles(Configuration hadoopConf, File hadoopConfigDir, List<ConfigIssue> issues) {
    boolean coreSiteExists = false;
    boolean hdfsSiteExists = false;
    boolean mapredSiteExists = false;
    boolean yarnSiteExists = false;
    File coreSite = new File(hadoopConfigDir, CORE_SITE_XML);
    if (coreSite.exists()) {
      coreSiteExists = true;
      if (!coreSite.isFile()) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
            CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsConfDir",
            Errors.HADOOPFS_27,
            coreSite.getPath()
        ));
      }
      hadoopConf.addResource(new Path(coreSite.getAbsolutePath()));
    }
    File hdfsSite = new File(hadoopConfigDir, HDFS_SITE_XML);
    if (hdfsSite.exists()) {
      hdfsSiteExists = true;
      if (!hdfsSite.isFile()) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
            CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsConfDir",
            Errors.HADOOPFS_27,
            hdfsSite.getPath()
        ));
      }
      hadoopConf.addResource(new Path(hdfsSite.getAbsolutePath()));
    }
    File yarnSite = new File(hadoopConfigDir, YARN_SITE_XML);
    if (yarnSite.exists()) {
      yarnSiteExists = true;
      if (!yarnSite.isFile()) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
            CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsConfDir",
            Errors.HADOOPFS_27,
            yarnSite.getPath()
        ));
      }
      hadoopConf.addResource(new Path(yarnSite.getAbsolutePath()));
    }
    File mapredSite = new File(hadoopConfigDir, MAPRED_SITE_XML);
    if (mapredSite.exists()) {
      mapredSiteExists = true;
      if (!mapredSite.isFile()) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
            CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsConfDir",
            Errors.HADOOPFS_27,
            mapredSite.getPath()
        ));
      }
      hadoopConf.addResource(new Path(mapredSite.getAbsolutePath()));
    }
    if (!coreSiteExists && shouldHadoopConfigsExists()) {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsConfDir",
          Errors.HADOOPFS_30,
          CORE_SITE_XML
      ));
    }
    if (!hdfsSiteExists && shouldHadoopConfigsExists()) {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsConfDir",
          Errors.HADOOPFS_30,
          HDFS_SITE_XML
      ));
    }
    if (!yarnSiteExists && shouldHadoopConfigsExists()) {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsConfDir",
          Errors.HADOOPFS_30,
          YARN_SITE_XML
      ));
    }
    if (!mapredSiteExists && shouldHadoopConfigsExists()) {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsConfDir",
          Errors.HADOOPFS_30,
          MAPRED_SITE_XML
      ));
    }
  }

  protected boolean shouldHadoopConfigsExists() {
    return true;
  }

  protected Configuration getHadoopConfiguration(List<ConfigIssue> issues) {
    Configuration hadoopConf = new Configuration();

    //We will handle the file system close ourselves in destroy
    //See https://issues.streamsets.com/browse/SDC-4057
    hadoopConf.setBoolean("fs.automatic.close", false);

    if (conf.hdfsKerberos) {
      hadoopConf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        UserGroupInformation.AuthenticationMethod.KERBEROS.name());
      try {
        hadoopConf.set(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, "hdfs/_HOST@" + HadoopSecurityUtil.getDefaultRealm());
      } catch (Exception ex) {
        if (!conf.hdfsConfigs.containsKey(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY)) {
          issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), null, Errors.HADOOPFS_28,
            ex.getMessage()));
        }
      }
    }
    if (conf.hdfsConfDir != null && !conf.hdfsConfDir.isEmpty()) {
      File hadoopConfigDir = new File(conf.hdfsConfDir);
      if (hadoopConfigDir.isAbsolute()) {
        // Do not allow absolute hadoop config directory in cluster mode
        issues.add(
            getContext().createConfigIssue(
                Groups.HADOOP_FS.name(),
                CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hadoopConfDir",
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
                  CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hadoopConfDir",
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
                CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hadoopConfDir",
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
                CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsConfDir",
                Errors.HADOOPFS_25,
                hadoopConfigDir.getPath()
            )
        );
      } else if (!hadoopConfigDir.isDirectory()) {
        issues.add(
            getContext().createConfigIssue(
                Groups.HADOOP_FS.name(),
                CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsConfDir",
                Errors.HADOOPFS_26,
                hadoopConfigDir.getPath()
            )
        );
      } else {
        validateHadoopConfigFiles(hadoopConf, hadoopConfigDir, issues);
      }
    } else if (shouldHadoopConfigsExists()) {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsConfDir",
          Errors.HADOOPFS_31
      ));
    }
    for (Map.Entry<String, String> config : conf.hdfsConfigs.entrySet()) {
      hadoopConf.set(config.getKey(), config.getValue());
    }
    return hadoopConf;
  }

  protected String getScheme() {
    return "hdfs";
  }


  @VisibleForTesting
  void validateHadoopFS(List<ConfigIssue> issues) {
    boolean validHapoopFsUri;
    String hdfsUriInConf;
    if (conf.hdfsUri != null && !conf.hdfsUri.isEmpty()) {
      hadoopConf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, conf.hdfsUri);
    } else {
      hdfsUriInConf = hadoopConf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
      if (hdfsUriInConf == null) {
        issues.add(
            getContext().createConfigIssue(
                Groups.HADOOP_FS.name(),
                CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsUri",
                Errors.HADOOPFS_19
            )
        );
        return;
      } else {
        conf.hdfsUri = hdfsUriInConf;
      }
    }
    validHapoopFsUri = validateHadoopFsURI(issues);
    StringBuilder logMessage = new StringBuilder();
    try {
      loginUgi = HadoopSecurityUtil.getLoginUser(hadoopConf);
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
      if (validHapoopFsUri) {
        getUGI().doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try (FileSystem fs = getFileSystemForInitDestroy()) { //to trigger the close
            }
            return null;
          }
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
    LOG.info("Authentication Config: " + logMessage);
  }

  @VisibleForTesting
  protected boolean validateHadoopFsURI(List<ConfigIssue> issues) {
    boolean validHapoopFsUri = true;
    if (conf.hdfsUri.contains("://")) {
      try {
        URI uri = new URI(conf.hdfsUri);
        if (!getScheme().equals(uri.getScheme())) {
          issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
              CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsUri",
              Errors.HADOOPFS_12,
              uri.getScheme(),
              getScheme()
          ));
          validHapoopFsUri = false;
        } else if (isURIAuthorityRequired() && uri.getAuthority() == null) {
          issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
              CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsUri",
              Errors.HADOOPFS_13,
              conf.hdfsUri
          ));
          validHapoopFsUri = false;
        }
      } catch (Exception ex) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
            CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsUri",
            Errors.HADOOPFS_22,
            conf.hdfsUri,
            ex.getMessage(),
            ex
        ));
        validHapoopFsUri = false;
      }
    } else {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(),
          CLUSTER_HDFS_CONFIG_BEAN_PREFIX + "hdfsUri",
          Errors.HADOOPFS_02,
          conf.hdfsUri
      ));
      validHapoopFsUri = false;
    }
    return validHapoopFsUri;
  }

  protected boolean isURIAuthorityRequired() {
    return true;
  }

  @VisibleForTesting
  FileSystem getFileSystemForInitDestroy() throws IOException {
    try {
      return getUGI().doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(new URI(conf.hdfsUri), hadoopConf);
        }
      });
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private UserGroupInformation getUGI() {
    return (conf.hdfsUser == null || conf.hdfsUser.isEmpty()) ?
        loginUgi : HadoopSecurityUtil.getProxyUser(conf.hdfsUser, loginUgi);
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
        if (count == 0 && DataFormat.DELIMITED == conf.dataFormat &&
            CsvHeader.NO_HEADER != conf.dataFormatConfig.csvHeader &&
            keyParts.length > 1 && keyParts[1].equals("0")) {
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
      List<Record> listRecords = null;
      if (conf.dataFormat == DataFormat.TEXT) {
        listRecords = processMessage(messageId, message.getValue());
      } else if (conf.dataFormat == DataFormat.DELIMITED) {
        switch (conf.dataFormatConfig.csvHeader) {
          case IGNORE_HEADER:
            // ignore header by skipping this header string
            // [1] - startOffset - [2] - contextKey
            String[] offsetContextSplit = messageId.split("::");
            if (offsetContextSplit.length > 1 && offsetContextSplit[1].equals("0")) {
              break;
            }
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
      } else if (conf.dataFormat == DataFormat.AVRO) {
        listRecords = processMessage(messageId, message.getValue());
      } else {
        throw new IllegalStateException(Utils.format("Unrecognized data format: '{}'", conf.dataFormat));
      }
      if (listRecords != null) {
        for (Record record : listRecords) {
          batchMaker.addRecord(record);
        }
      }
    }
    if (count == 0) {
      LOG.info("Received no records, returning null");
      return null;
    }
    return Utils.checkNotNull(messageId, "Log error, message ID cannot be null at this point.");
  }

  protected List<Record> processMessage(String messageId, Object message) throws StageException {
    List<Record> records = new ArrayList<>();
    if (conf.dataFormat == DataFormat.AVRO) {
      try (DataParser parser = parserFactory.getParser(messageId, (byte[]) message)) {
        Record record = parser.parse();
        if (record != null) {
          records.add(record);
        }
      } catch (IOException | DataParserException ex) {
        LOG.debug("Got exception: '{}'", ex, ex);
        errorRecordHandler.onError(Errors.HADOOPFS_08, messageId, ex.toString(), ex);
      }
    } else {
      try (DataParser parser = parserFactory.getParser(messageId, String.valueOf(message))) {
        Record record = parser.parse();
        while (record != null) {
          records.add(record);
          record = parser.parse();
        }
      } catch (IOException | DataParserException ex) {
        LOG.debug("Got exception: '{}'", ex, ex);
        errorRecordHandler.onError(Errors.HADOOPFS_08, messageId, ex.toString(), ex);
      }
    }
    if (conf.produceSingleRecordPerMessage) {
      List<Field> list = new ArrayList<>();
      for (Record record : records) {
        list.add(record.get());
      }
      Record record = records.get(0);
      record.set(Field.create(list));
      records.clear();
      records.add(record);
    }
    return records;
  }

  @Override
  public void put(List<Map.Entry> batch) throws InterruptedException {
    producer.put(new OffsetAndResult<>(recordsProduced, batch));
    recordsProduced += batch.size();
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
      LOG.warn("Thread interrupted while waiting on receving the done flag" + e, e);
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
    Map<String, String> configsToShip = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : hadoopConf) {
      // hadoopConf.get() is required since entry.getValue()
      // does not have variables expanded
      configsToShip.put(entry.getKey(), hadoopConf.get(entry.getKey()));
    }
    return configsToShip;
  }
  @Override
  public void postDestroy() {
    countDownLatch.countDown();
  }
}
