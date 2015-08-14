/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.streamsets.pipeline.cluster.Consumer;
import com.streamsets.pipeline.cluster.ControlChannel;
import com.streamsets.pipeline.cluster.DataChannel;
import com.streamsets.pipeline.cluster.Producer;
import com.streamsets.pipeline.impl.Pair;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
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
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;

import javax.security.auth.Subject;

public class ClusterHdfsSource extends BaseSource implements OffsetCommitter, ErrorListener, ClusterSource {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterHdfsSource.class);
  private static final int PREVIEW_SIZE = 100;
  private String hdfsUri;
  private final List<String> hdfsDirLocations;
  private Configuration hadoopConf;
  private final ControlChannel controlChannel;
  private final DataChannel dataChannel;
  private final Producer producer;
  private final Consumer consumer;
  private final DataFormat dataFormat;
  private final int textMaxLineLen;
  private final int jsonMaxObjectLen;
  private final LogMode logMode;
  private final boolean retainOriginalLine;
  private final String customLogFormat;
  private final String regex;
  private final List<RegExConfig> fieldPathsToGroupName;
  private final String grokPatternDefinition;
  private final String grokPattern;
  private final boolean enableLog4jCustomLogFormat;
  private final String log4jCustomLogFormat;
  private LogDataFormatValidator logDataFormatValidator;
  private final int logMaxObjectLen;
  private DataParserFactory parserFactory;
  private final boolean produceSingleRecordPerMessage;
  private final Map<String, String> hdfsConfigs;
  private final boolean hdfsKerberos;
  private final String hdfsUser;
  private final String hadoopConfDir;
  private UserGroupInformation loginUgi;
  private final boolean recursive;
  private long recordsProduced;
  private final Map<String, String> previewBuffer;
  private final CountDownLatch countDownLatch;

  public ClusterHdfsSource(String hdfsUri, List<String> hdfsDirLocations, boolean recursive, Map<String, String> hdfsConfigs, DataFormat dataFormat, int textMaxLineLen,
    int jsonMaxObjectLen, LogMode logMode, boolean retainOriginalLine, String customLogFormat, String regex,
    List<RegExConfig> fieldPathsToGroupName, String grokPatternDefinition, String grokPattern,
    boolean enableLog4jCustomLogFormat, String log4jCustomLogFormat, int logMaxObjectLen, boolean produceSingleRecordPerMessage,
    boolean hdfsKerberos, String hdfsUser, String hadoopConfDir) {
    controlChannel = new ControlChannel();
    dataChannel = new DataChannel();
    producer = new Producer(controlChannel, dataChannel);
    consumer = new Consumer(controlChannel, dataChannel);
    this.recursive = recursive;
    this.hdfsConfigs = hdfsConfigs;
    this.hdfsUri = hdfsUri;
    this.hdfsDirLocations = hdfsDirLocations;
    this.dataFormat = dataFormat;
    this.textMaxLineLen = textMaxLineLen;
    this.jsonMaxObjectLen = jsonMaxObjectLen;
    this.logMode = logMode;
    this.retainOriginalLine = retainOriginalLine;
    this.customLogFormat = customLogFormat;
    this.regex = regex;
    this.fieldPathsToGroupName = fieldPathsToGroupName;
    this.enableLog4jCustomLogFormat = enableLog4jCustomLogFormat;
    this.grokPatternDefinition = grokPatternDefinition;
    this.grokPattern = grokPattern;
    this.log4jCustomLogFormat = log4jCustomLogFormat;
    this.logMaxObjectLen = logMaxObjectLen;
    this.produceSingleRecordPerMessage = produceSingleRecordPerMessage;
    this.hdfsKerberos = hdfsKerberos;
    this.hdfsUser = hdfsUser;
    this.hadoopConfDir = hadoopConfDir;
    this.recordsProduced = 0;
    this.previewBuffer = new LinkedHashMap<>();
    this.countDownLatch = new CountDownLatch(1);
  }
  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    validateHadoopFS(issues);
    // This is for getting no of splits - no of executors
    hadoopConf.set(FileInputFormat.LIST_STATUS_NUM_THREADS, "5"); // Per Hive-on-Spark
    hadoopConf.set(FileInputFormat.SPLIT_MAXSIZE, String.valueOf(750000000)); // Per Hive-on-Spark
    for (Map.Entry<String, String> config : hdfsConfigs.entrySet()) {
      hadoopConf.set(config.getKey(), config.getValue());
    }
    List<Path> hdfsDirPaths = new ArrayList<>();
    if (hdfsDirLocations == null || hdfsDirLocations.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsDirLocations", Errors.HADOOPFS_18));
    } else if (issues.isEmpty()) {
      for (String hdfsDirLocation : hdfsDirLocations) {
        try {
          FileSystem fs = getFileSystemForInitDestroy();
          Path ph = fs.makeQualified(new Path(hdfsDirLocation));
          hdfsDirPaths.add(ph);
          if (!fs.exists(ph)) {
            issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsDirLocations", Errors.HADOOPFS_10,
              hdfsDirLocation));
          } else if (!fs.getFileStatus(ph).isDirectory()) {
            issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsDirLocations", Errors.HADOOPFS_15,
              hdfsDirLocation));
          } else {
            try {
              FileStatus[] files = fs.listStatus(ph);
              if (files == null || files.length == 0) {
                issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsDirLocations", Errors.HADOOPFS_16,
                  hdfsDirLocation));
              } else if (getContext().isPreview() && previewBuffer.size() < PREVIEW_SIZE) {
                for (FileStatus fileStatus : files) {
                  if (fileStatus.isFile()) {
                    InputStream in = null;
                    BufferedReader reader = null;
                    String path = fileStatus.getPath().toString();
                    try {
                      in =  fs.open(fileStatus.getPath());
                      reader = new BufferedReader(new InputStreamReader(in));
                      String line;
                      int offset = 0;
                      while ((line = reader.readLine()) != null && previewBuffer.size() < PREVIEW_SIZE) {
                        previewBuffer.put(path + "::" + offset, line);
                        offset += line.getBytes(StandardCharsets.UTF_8).length + 1; // byte length and newline
                      }
                    } catch (IOException ex) {
                      String msg = "Error opening " + path + ": " + ex;
                      LOG.info(msg, ex);
                      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsDirLocations", Errors.HADOOPFS_16,
                        fileStatus.getPath()));
                    } finally {
                      IOUtils.closeQuietly(in);
                      IOUtils.closeQuietly(reader);
                    }
                  }
                }
              }
            } catch (IOException ex) {
              issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsDirLocations", Errors.HADOOPFS_09,
                hdfsDirLocation, ex.toString(), ex));
            }
          }
        } catch (IOException ioe) {
          LOG.warn("Error connecting to HDFS filesystem: " + ioe, ioe);
          issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsDirLocations", Errors.HADOOPFS_11,
            hdfsDirLocation, ioe.toString(), ioe));
        }
      }
    }
    hadoopConf.set(FileInputFormat.INPUT_DIR, StringUtils.join(hdfsDirPaths, ","));
    hadoopConf.set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.toString(recursive));
    switch (dataFormat) {
      case JSON:
        if (jsonMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.JSON.name(), "jsonMaxObjectLen", Errors.HADOOPFS_04));
        }
        break;
      case TEXT:
        if (textMaxLineLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.TEXT.name(), "textMaxLineLen", Errors.HADOOPFS_05));
        }
        break;
      case LOG:
        logDataFormatValidator = new LogDataFormatValidator(logMode, logMaxObjectLen, retainOriginalLine,
                                                            customLogFormat, regex, grokPatternDefinition, grokPattern,
                                                            enableLog4jCustomLogFormat, log4jCustomLogFormat,
                                                            OnParseError.ERROR, 0, Groups.LOG.name(),
                                                            getFieldPathToGroupMap(fieldPathsToGroupName));
        logDataFormatValidator.validateLogFormatConfig(issues, getContext());
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.LOG.name(), "dataFormat", Errors.HADOOPFS_06, dataFormat));
    }
    validateParserFactoryConfigs(issues);
    LOG.info("Issues: " + issues);
    return issues;
  }

  @VisibleForTesting
  Configuration getConfiguration() {
    return hadoopConf;
  }

  Configuration getHadoopConfiguration(List<ConfigIssue> issues) {
    Configuration conf = new Configuration();
    if (hdfsKerberos) {
      conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        UserGroupInformation.AuthenticationMethod.KERBEROS.name());
      try {
        conf.set(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, "hdfs/_HOST@" + KerberosUtil.getDefaultRealm());
      } catch (Exception ex) {
        if (!hdfsConfigs.containsKey(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY)) {
          issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), null, Errors.HADOOPFS_28,
            ex.getMessage()));
        }
      }
    }
    if (hadoopConfDir != null && !hadoopConfDir.isEmpty()) {
      File hadoopConfigDir = new File(hadoopConfDir);
      if (!hadoopConfigDir.isAbsolute()) {
        hadoopConfigDir = new File(getContext().getResourcesDirectory(), hadoopConfDir).getAbsoluteFile();
      }
      if (!hadoopConfigDir.exists()) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsConfDir", Errors.HADOOPFS_25,
          hadoopConfigDir.getPath()));
      } else if (!hadoopConfigDir.isDirectory()) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsConfDir", Errors.HADOOPFS_26,
          hadoopConfigDir.getPath()));
      } else {
        File coreSite = new File(hadoopConfigDir, "core-site.xml");
        if (coreSite.exists()) {
          if (!coreSite.isFile()) {
            issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsConfDir", Errors.HADOOPFS_27,
              coreSite.getPath()));
          }
          conf.addResource(new Path(coreSite.getAbsolutePath()));
        }
        File hdfsSite = new File(hadoopConfigDir, "hdfs-site.xml");
        if (hdfsSite.exists()) {
          if (!hdfsSite.isFile()) {
            issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsConfDir", Errors.HADOOPFS_27,
              hdfsSite.getPath()));
          }
          conf.addResource(new Path(hdfsSite.getAbsolutePath()));
        }
        File yarnSite = new File(hadoopConfigDir, "yarn-site.xml");
        if (yarnSite.exists()) {
          if (!yarnSite.isFile()) {
            issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsConfDir", Errors.HADOOPFS_27,
              yarnSite.getPath()));
          }
          conf.addResource(new Path(yarnSite.getAbsolutePath()));
        }
        File mapredSite = new File(hadoopConfigDir, "mapred-site.xml");
        if (mapredSite.exists()) {
          if (!mapredSite.isFile()) {
            issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsConfDir", Errors.HADOOPFS_27,
              mapredSite.getPath()));
          }
          conf.addResource(new Path(mapredSite.getAbsolutePath()));
        }
      }
    }
    for (Map.Entry<String, String> config : hdfsConfigs.entrySet()) {
      conf.set(config.getKey(), config.getValue());
    }
    return conf;
  }


  private void validateHadoopFS(List<ConfigIssue> issues) {
    boolean validHapoopFsUri = true;
    hadoopConf = getHadoopConfiguration(issues);
    String hdfsUriInConf;
    if (hdfsUri != null && !hdfsUri.isEmpty()) {
      hadoopConf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hdfsUri);
    } else {
      hdfsUriInConf = hadoopConf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
      if (hdfsUriInConf == null) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsUri", Errors.HADOOPFS_19));
        return;
      } else {
        hdfsUri = hdfsUriInConf;
      }
    }
    if (hdfsUri.contains("://")) {
      try {
        URI uri = new URI(hdfsUri);
        if (!"hdfs".equals(uri.getScheme())) {
          issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsUri", Errors.HADOOPFS_12, hdfsUri,
            uri.getScheme()));
          validHapoopFsUri = false;
        } else if (uri.getAuthority() == null) {
          issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsUri", Errors.HADOOPFS_13, hdfsUri));
          validHapoopFsUri = false;
        }
      } catch (Exception ex) {
        issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsUri", Errors.HADOOPFS_22, hdfsUri,
          ex.getMessage(), ex));
        validHapoopFsUri = false;
      }
    } else {
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsUri", Errors.HADOOPFS_02, hdfsUri));
      validHapoopFsUri = false;
    }

    StringBuilder logMessage = new StringBuilder();
    try {
      // forcing UGI to initialize with the security settings from the stage
      UserGroupInformation.setConfiguration(hadoopConf);

      // If Kerberos is enabled the SDC is already logged to the KDC, we need to UGI login using the SDC login context
      UserGroupInformation.loginUserFromSubject(Subject.getSubject(AccessController.getContext()));
      // we now extract the UGI we just logged in as.
      loginUgi = UserGroupInformation.getLoginUser();

      if (hdfsKerberos) {
        logMessage.append("Using Kerberos");
        if (loginUgi.getAuthenticationMethod() != UserGroupInformation.AuthenticationMethod.KERBEROS) {
          issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), "hdfsKerberos", Errors.HADOOPFS_00,
            loginUgi.getAuthenticationMethod(),
            UserGroupInformation.AuthenticationMethod.KERBEROS));
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
      issues.add(getContext().createConfigIssue(Groups.HADOOP_FS.name(), null, Errors.HADOOPFS_11, hdfsUri,
        String.valueOf(ex), ex));
    }
    LOG.info("Authentication Config: " + logMessage);
  }

  private FileSystem getFileSystemForInitDestroy() throws IOException {
    try {
      return getUGI().doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(new URI(hdfsUri), hadoopConf);
        }
      });
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private UserGroupInformation getUGI() {
    return (hdfsUser == null || hdfsUser.isEmpty()) ? loginUgi : UserGroupInformation.createProxyUser(hdfsUser, loginUgi);
  }

  private void validateParserFactoryConfigs(List<ConfigIssue> issues) {
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(getContext(), dataFormat.getParserFormat())
      .setCharset(Charset.defaultCharset());

    // TextInputFormat supports Hadoop Text class which is Standard UTF-8
    builder.setCharset(StandardCharsets.UTF_8);

    switch (dataFormat) {
      case TEXT:
        builder.setMaxDataLen(textMaxLineLen);
        break;
      case JSON:
        builder.setMode(JsonMode.MULTIPLE_OBJECTS);
        builder.setMaxDataLen(jsonMaxObjectLen);
        break;
      case LOG:
        logDataFormatValidator.populateBuilder(builder);
        break;
    }
    parserFactory = builder.build();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    OffsetAndResult<Map.Entry> offsetAndResult;
    if (getContext().isPreview()) {
      // we only support text today
      List<Map.Entry> records = new ArrayList<>();
      int count = 0;
      Iterator<String> keys = previewBuffer.keySet().iterator();
      while (count < maxBatchSize && count < previewBuffer.size() && keys.hasNext()) {
        String key =  keys.next();
        records.add(new Pair(key, previewBuffer.get(key)));
        count++;
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
    for (Map.Entry message : offsetAndResult.getResult()) {
      count++;
      messageId = String.valueOf(message.getKey());
      List<Record> listRecords = processMessage(messageId, String.valueOf(message.getValue()));
      for (Record record : listRecords) {
        batchMaker.addRecord(record);
      }
    }
    if (count == 0) {
      LOG.info("Received no records, returning null");
      return null;
    }
    return Utils.checkNotNull(messageId, "Log error, message ID cannot be null at this point.");
  }


  protected List<Record> processMessage(String messageId, String message) throws StageException {
    List<Record> records = new ArrayList<>();
    try (DataParser parser = parserFactory.getParser(messageId, message)) {
      Record record = parser.parse();
      while (record != null) {
        records.add(record);
        record = parser.parse();
      }
    } catch (IOException|DataParserException ex) {
      handleException(messageId, ex);
    }
    if (produceSingleRecordPerMessage) {
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


  private Map<String, Integer> getFieldPathToGroupMap(List<RegExConfig> fieldPathsToGroupName) {
    if(fieldPathsToGroupName == null) {
      return new HashMap<>();
    }
    Map<String, Integer> fieldPathToGroup = new HashMap<>();
    for(RegExConfig r : fieldPathsToGroupName) {
      fieldPathToGroup.put(r.fieldPath, r.group);
    }
    return fieldPathToGroup;
  }

  private void handleException(String messageId, Exception ex) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().reportError(Errors.HADOOPFS_08, messageId, ex.toString(), ex);
        break;
      case STOP_PIPELINE:
        if (ex instanceof StageException) {
          throw (StageException) ex;
        } else {
          throw new StageException(Errors.HADOOPFS_08, messageId, ex.toString(), ex);
        }
      default:
        throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
          getContext().getOnErrorRecord(), ex));
    }
  }

  @Override
  public int getParallelism() throws IOException {
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
