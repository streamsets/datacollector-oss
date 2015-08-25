/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.AbortedException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.impl.XMLChar;
import com.streamsets.pipeline.lib.io.ObjectLengthException;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.avro.AvroDataParserFactory;
import com.streamsets.pipeline.lib.parser.delimited.DelimitedDataParserFactory;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataFormatConfig;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AmazonS3Source extends BaseSource {

  private final static Logger LOG = LoggerFactory.getLogger(AmazonS3Source.class);

  private static final String OFFSET_SEPARATOR = "::";
  private static final String MINUS_ONE = "-1";
  private static final String ZERO = "0";

  private final BasicConfig basicConfig;
  private final DataFormatConfig dataFormatConfig;
  private final S3ErrorConfig errorConfig;
  private final S3PostProcessingConfig postProcessingConfig;
  private final S3FileConfig s3FileConfig;
  private final S3Config s3Config;

  private AmazonS3Client s3Client;
  private S3Spooler spooler;

  public AmazonS3Source(S3ConfigBean s3ConfigBean) {
    this.basicConfig = s3ConfigBean.basicConfig;
    this.dataFormatConfig = s3ConfigBean.dataFormatConfig;
    this.errorConfig = s3ConfigBean.errorConfig;
    this.postProcessingConfig = s3ConfigBean.postProcessingConfig;
    this.s3FileConfig = s3ConfigBean.s3FileConfig;
    this.s3Config = s3ConfigBean.s3Config;
  }

  private Charset fileCharset;
  private S3ObjectSummary currentObject;
  private DataParserFactory parserFactory;
  private DataParser parser;
  S3Object object;
  private LogDataFormatValidator logDataFormatValidator;

  public S3ObjectSummary getCurrentObject() {
    return currentObject;
  }

  public void setCurrentObject(S3ObjectSummary currentObject) {
    this.currentObject = currentObject;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    validateConnection(issues);

    validateBucket(s3Config.bucket, Groups.S3.name(), "bucket", issues);

    basicConfig.validate(issues, Groups.S3.name(), getContext());
    s3FileConfig.validate(getContext(), issues);

    if (errorConfig.errorBucket != null && !errorConfig.errorBucket.isEmpty()) {
      validateBucket(errorConfig.errorBucket, Groups.ERROR_HANDLING.name(), "errorBucket", issues);
    }

    switch (dataFormatConfig.dataFormat) {
      case JSON:
        if (dataFormatConfig.jsonMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.JSON.name(), "jsonMaxObjectLen", Errors.S3_SPOOLDIR_14));
        }
        break;
      case TEXT:
        if (dataFormatConfig.textMaxLineLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.TEXT.name(), "textMaxLineLen", Errors.S3_SPOOLDIR_14));
        }
        break;
      case DELIMITED:
        if (dataFormatConfig.csvMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.DELIMITED.name(), "csvMaxObjectLen", Errors.S3_SPOOLDIR_14));
        }
        break;
      case XML:
        if (dataFormatConfig.xmlMaxObjectLen < 1) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "xmlMaxObjectLen", Errors.S3_SPOOLDIR_14));
        }
        if (dataFormatConfig.xmlRecordElement == null || dataFormatConfig.xmlRecordElement.isEmpty()) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "xmlRecordElement", Errors.S3_SPOOLDIR_23));
        } else if (!XMLChar.isValidName(dataFormatConfig.xmlRecordElement)) {
          issues.add(getContext().createConfigIssue(Groups.XML.name(), "xmlRecordElement", Errors.S3_SPOOLDIR_21,
            dataFormatConfig.xmlRecordElement));
        }
        break;
      case SDC_JSON:
        break;
      case AVRO:
        break;
      case LOG:
        logDataFormatValidator = new LogDataFormatValidator(dataFormatConfig.logMode, dataFormatConfig.logMaxObjectLen,
          dataFormatConfig.retainOriginalLine, dataFormatConfig.customLogFormat, dataFormatConfig.regex,
          dataFormatConfig.grokPatternDefinition, dataFormatConfig.grokPattern,
          dataFormatConfig.enableLog4jCustomLogFormat, dataFormatConfig.log4jCustomLogFormat,
          dataFormatConfig.onParseError, dataFormatConfig.maxStackTraceLines, Groups.LOG.name(),
          getFieldPathToGroupMap(dataFormatConfig.fieldPathsToGroupName));
        logDataFormatValidator.validateLogFormatConfig(issues, getContext());
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.S3.name(), "dataFormat", Errors.S3_SPOOLDIR_10,
          dataFormatConfig.dataFormat));
        break;
    }

    validateDataParser(issues);

    if (getContext().isPreview()) {
      basicConfig.maxWaitTime = 1000;
    }

    if (issues.isEmpty()) {
      spooler = new S3Spooler(getContext(), s3FileConfig, s3Config, postProcessingConfig, errorConfig, s3Client);
      spooler.init();
    }

    return issues;
  }

  private void validateConnection(List<ConfigIssue> issues) {
    //Access Key ID - username [unique in aws]
    //secret access key - password
    AWSCredentials credentials = new BasicAWSCredentials(s3Config.accessKeyId, s3Config.secretAccessKey);
    s3Client = new AmazonS3Client(credentials, new ClientConfiguration());
    s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    s3Client.setRegion(Region.getRegion(s3Config.region));
    try {
      //check if the credentials are right by trying to list buckets
      s3Client.listBuckets();
    } catch (AmazonS3Exception e) {
      issues.add(getContext().createConfigIssue(Groups.S3.name(), "accessKeyId", Errors.S3_SPOOLDIR_30,
        e.toString()));
    }
  }

  private void validateBucket(String bucket, String group, String config, List<ConfigIssue> issues) {
    if(bucket == null || bucket.isEmpty()) {
      issues.add(getContext().createConfigIssue(group, config, Errors.S3_SPOOLDIR_11));
    }
    //check if the bucket name is valid
    if (!s3Client.doesBucketExist(bucket)) {
      issues.add(getContext().createConfigIssue(group, config, Errors.S3_SPOOLDIR_12, bucket));
    }
  }

  private void validateDataParser(List<ConfigIssue> issues) {
    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(getContext(),
      dataFormatConfig.dataFormat.getParserFormat());

    try {
      fileCharset = Charset.forName(dataFormatConfig.charset);
    } catch (UnsupportedCharsetException ex) {
      // setting it to a valid one so the parser factory can be configured and tested for more errors
      fileCharset = StandardCharsets.UTF_8;
      issues.add(getContext().createConfigIssue(Groups.S3.name(), "charset", Errors.S3_SPOOLDIR_05,
        dataFormatConfig.charset));
    }
    builder.setCharset(fileCharset);
    builder.setOverRunLimit(s3FileConfig.overrunLimit);
    builder.setRemoveCtrlChars(dataFormatConfig.removeCtrlChars);

    switch (dataFormatConfig.dataFormat) {
      case TEXT:
        builder.setMaxDataLen(dataFormatConfig.textMaxLineLen);
        break;
      case JSON:
        builder.setMaxDataLen(dataFormatConfig.jsonMaxObjectLen).setMode(dataFormatConfig.jsonContent);
        break;
      case DELIMITED:
        builder.setMaxDataLen(dataFormatConfig.csvMaxObjectLen).setMode(dataFormatConfig.csvFileFormat)
          .setMode(dataFormatConfig.csvHeader).setMode(dataFormatConfig.csvRecordType)
          .setConfig(DelimitedDataParserFactory.DELIMITER_CONFIG, dataFormatConfig.csvCustomDelimiter)
          .setConfig(DelimitedDataParserFactory.ESCAPE_CONFIG, dataFormatConfig.csvCustomEscape)
          .setConfig(DelimitedDataParserFactory.QUOTE_CONFIG, dataFormatConfig.csvCustomQuote);
        break;
      case XML:
        builder.setMaxDataLen(dataFormatConfig.xmlMaxObjectLen).setConfig(XmlDataParserFactory.RECORD_ELEMENT_KEY,
          dataFormatConfig.xmlRecordElement);
        break;
      case SDC_JSON:
        builder.setMaxDataLen(-1);
        s3FileConfig.maxSpoolObjects = 10000;
        break;
      case LOG:
        logDataFormatValidator.populateBuilder(builder);
        break;
      case AVRO:
        builder.setMaxDataLen(-1).setConfig(AvroDataParserFactory.SCHEMA_KEY, dataFormatConfig.avroSchema);
        break;
    }
    try {
      parserFactory = builder.build();
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(null, null, Errors.S3_SPOOLDIR_22, ex.toString(), ex));
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

  @Override
  public void destroy() {
    IOUtils.closeQuietly(parser);
    if(s3Client != null) {
      s3Client.shutdown();
    }
    if(spooler != null) {
      spooler.destroy();
    }
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int batchSize = Math.min(basicConfig.maxBatchSize, maxBatchSize);

    //parse offset string into S3Offset data structure
    S3Offset s3Offset = S3Offset.fromString(lastSourceOffset);

    spooler.postProcessOlderObjectIfNeeded(s3Offset);

    //check if we have an object to produce records from. Otherwise get from spooler.
    if (needToFetchNextObjectFromSpooler(s3Offset)) {
      s3Offset = fetchNextObjectFromSpooler(s3Offset);
      LOG.debug("Object '{}' with offset '{}' fetched from Spooler", s3Offset.getKey(), s3Offset.getOffset());
    } else {
      //check if the current object was modified between batches
      LOG.debug("Checking if Object '{}' has been modified between batches", getCurrentObject().getKey());
      if (!getCurrentObject().getETag().equals(s3Offset.geteTag())) {
        //send the current object to error archive and get next object from spooler
        LOG.debug("Object '{}' has been modified between batches. Sending the object to error",
          getCurrentObject().getKey());
        try {
          spooler.handleCurrentObjectAsError();
        } catch (AmazonClientException e) {
          throw new StageException(Errors.S3_SPOOLDIR_34, e.toString(), e);
        }
        s3Offset = fetchNextObjectFromSpooler(s3Offset);
      }
    }

    if (getCurrentObject() != null) {
      try {
        // we ask for a batch from the currentObject starting at offset
        s3Offset.setOffset(produce(getCurrentObject().getKey(), s3Offset.getOffset(), batchSize, batchMaker));
      } catch (BadSpoolObjectException ex) {
        LOG.error(Errors.S3_SPOOLDIR_01.getMessage(), ex.getObject(), ex.getPos(), ex.toString(), ex);
        getContext().reportError(Errors.S3_SPOOLDIR_01, ex.getObject(), ex.getPos(), ex.toString());
        try {
          spooler.handleCurrentObjectAsError();
        } catch (AmazonClientException e) {
          throw new StageException(Errors.S3_SPOOLDIR_34, e.toString(), e);
        }
        // we set the offset to -1 to indicate we are done with the current object and we should fetch a new one
        // from the spooler
        s3Offset.setOffset(MINUS_ONE);
      }
    }
    return s3Offset.toString();
  }

  private S3Offset fetchNextObjectFromSpooler(S3Offset s3Offset) throws StageException {
    setCurrentObject(null);
    try {
      //The next object found in queue is mostly eligible since we process objects in chronological order.

      //However after processing a few files, if the configuration is changed [say relax the prefix] and an older file
      //gets selected for processing, it must be ignored.
      S3ObjectSummary nextAvailObj = null;
      do {
        if (nextAvailObj != null) {
          LOG.warn("Ignoring object '{}' in spool directory as is lesser than offset object '{}'",
            nextAvailObj.getKey(), s3Offset.getKey());
        }
        nextAvailObj = spooler.poolForObject(basicConfig.maxWaitTime, TimeUnit.MILLISECONDS);
      } while (!isEligible(nextAvailObj, s3Offset));

      if (nextAvailObj == null) {
        // no object to process
        LOG.debug("No new object available in spool directory after '{}' secs, producing empty batch",
          basicConfig.maxWaitTime/1000);
      } else {
        setCurrentObject(nextAvailObj);

        // if the current offset object is null or the object returned by the spooler is greater than the current offset
        // object we take the object returned by the spooler as the new object and set the offset to zero.
        // if not, it means the spooler returned us the current object, we just keep processing it from the last
        // offset we processed (known via offset tracking)
        if (s3Offset.getKey() == null ||
          isLaterThan(nextAvailObj.getKey(), nextAvailObj.getLastModified().getTime(), s3Offset.getKey(),
            Long.parseLong(s3Offset.getTimestamp()))) {
          s3Offset = new S3Offset(getCurrentObject().getKey(), ZERO, getCurrentObject().getETag(),
            String.valueOf(getCurrentObject().getLastModified().getTime()));
        }
      }
    } catch (InterruptedException ex) {
      // the spooler was interrupted while waiting for an object, we log and return, the pipeline agent will invoke us
      // again to wait for an object again
      LOG.warn("Pooling interrupted");
    } catch (AmazonClientException e) {
      throw new StageException(Errors.S3_SPOOLDIR_33, e.toString());
    }
    return s3Offset;
  }

  public String produce(String objectKey, String offset, int maxBatchSize, BatchMaker batchMaker) throws StageException,
    BadSpoolObjectException {
    try {
      if (parser == null) {
        //Get S3 object instead of stream because we want to call close on the object when we close the
        // parser (and stream)
        object = AmazonS3Util.getObject(s3Client, s3Config.bucket, objectKey);
        parser = parserFactory.getParser(objectKey, object.getObjectContent(), Long.parseLong(offset));
        //we don't use S3 GetObject range capabilities to skip the already process offset because the parsers cannot
        // pick up from a non root doc depth in the case of a single object with records.
      }
      for (int i = 0; i < maxBatchSize; i++) {
        try {
          Record record = parser.parse();
          if (record != null) {
            batchMaker.addRecord(record);
            offset = parser.getOffset();
          } else {
            parser.close();
            parser = null;
            object.close();
            object = null;
            offset = MINUS_ONE;
            break;
          }
        } catch (ObjectLengthException ex) {
          String exOffset = offset;
          offset = MINUS_ONE;
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().reportError(Errors.S3_SPOOLDIR_02, objectKey, exOffset);
              break;
            case STOP_PIPELINE:
              throw new StageException(Errors.S3_SPOOLDIR_02, objectKey, exOffset);
            default:
              throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                getContext().getOnErrorRecord(), ex));
          }
        }
      }
    } catch (AmazonClientException e) {
      LOG.error("Error processing object with key '{}' offset '{}'", objectKey, offset);
      throw new StageException(Errors.S3_SPOOLDIR_35, e.toString());
    } catch (IOException | DataParserException ex) {
      if(ex.getCause() instanceof AbortedException) {
        //If the pipeline was stopped, the amazon s3 client thread catches the interrupt and throws aborted exception
        //do not treat this as an error. Instead produce what ever you have and move one.

      } else {
        offset = MINUS_ONE;
        String exOffset;
        if (ex instanceof OverrunException) {
          exOffset = String.valueOf(((OverrunException) ex).getStreamOffset());
        } else {
          try {
            exOffset = (parser != null) ? parser.getOffset() : MINUS_ONE;
          } catch (IOException ex1) {
            LOG.warn("Could not get the object offset to report with error, reason: {}", ex1.toString(), ex);
            exOffset = MINUS_ONE;
          }
        }

        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            throw new BadSpoolObjectException(objectKey, exOffset, ex);
          case STOP_PIPELINE:
            getContext().reportError(Errors.S3_SPOOLDIR_04, objectKey, exOffset, ex.toString());
            throw new StageException(Errors.S3_SPOOLDIR_04, objectKey, exOffset, ex.toString(), ex);
          default:
            throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
              getContext().getOnErrorRecord(), ex));
        }
      }
    } finally {
      if (MINUS_ONE.equals(offset)) {
        if (parser != null) {
          try {
            parser.close();
            parser = null;
          } catch (IOException ex) {
            LOG.debug("Exception while closing parser : '{}'", ex.toString());
          }
        }
        if (object != null) {
          try {
            object.close();
            object = null;
          } catch (IOException ex) {
            LOG.debug("Exception while closing S3 object : '{}'", ex.toString());
          }
        }
      }
    }
    return offset;
  }

  private boolean needToFetchNextObjectFromSpooler(S3Offset s3Offset) {
    return
      // we don't have an object half way processed in the current agent execution
      getCurrentObject() == null ||
      // we don't have an object half way processed from a previous agent execution via offset tracking
      s3Offset.getKey() == null ||
      // the current object has been fully processed
      MINUS_ONE.equals(s3Offset.getOffset());
  }

  private boolean isEligible(S3ObjectSummary nextAvailObj, S3Offset s3Offset) {
    return (nextAvailObj == null) ||
      (nextAvailObj.getLastModified().getTime() >= Long.parseLong(s3Offset.getTimestamp()));
  }

  private boolean isLaterThan(String nextKey, long nextTimeStamp, String originalKey, long originalTimestamp) {
    return (nextTimeStamp > originalTimestamp) ||
      (nextTimeStamp == originalTimestamp && nextKey.compareTo(originalKey) > 0);
  }

  static class S3Offset {
    private final String key;
    private final String eTag;
    private String offset;
    private final String timestamp;

    public S3Offset(String key, String offset, String eTag, String timestamp) {
      this.key = key;
      this.offset = offset;
      this.eTag = eTag;
      this.timestamp = timestamp;
    }

    public String getKey() {
      return key;
    }

    public String geteTag() {
      return eTag;
    }

    public String getOffset() {
      return offset;
    }

    public String getTimestamp() {
      return timestamp;
    }

    public void setOffset(String offset) {
      this.offset = offset;
    }

    @Override
    public String toString() {
      return key + OFFSET_SEPARATOR + offset + OFFSET_SEPARATOR + eTag + OFFSET_SEPARATOR + timestamp;
    }

    public static S3Offset fromString(String lastSourceOffset) throws StageException {
      if (lastSourceOffset != null) {
        String[] split = lastSourceOffset.split(OFFSET_SEPARATOR);
        if (split.length == 4) {
          return new S3Offset(split[0], split[1], split[2], split[3]);
        } else {
          throw new StageException(Errors.S3_SPOOLDIR_31, lastSourceOffset);
        }
      }
      return new S3Offset(null, ZERO, null, ZERO);
    }
  }

}
