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
package com.streamsets.pipeline.stage.origin.remote;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.io.ObjectLengthException;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileNotFoundException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;

import static com.streamsets.pipeline.stage.origin.lib.DataFormatParser.DATA_FORMAT_CONFIG_PREFIX;

public class RemoteDownloadSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteDownloadSource.class);
  private static final String OFFSET_DELIMITER = "::";
  private static final String CONF_PREFIX = "conf.";
  private static final String REMOTE_ADDRESS_CONF = CONF_PREFIX + "remoteAddress";
  private static final String MINUS_ONE = "-1";
  private static final String ZERO = "0";

  static final String NOTHING_READ = "null";

  static final String SIZE = "size";
  static final String LAST_MODIFIED_TIME = "lastModifiedTime";
  static final String REMOTE_URI = "remoteUri";
  static final String CONTENT_TYPE = "contentType";
  static final String CONTENT_ENCODING = "contentEncoding";


  private final RemoteDownloadConfigBean conf;
  private final File knownHostsFile;
  private final File errorArchive;
  private final byte[] moveBuffer;

  private RemoteFile next = null;
  private ELEval rateLimitElEval;
  private ELVars rateLimitElVars;


  private final NavigableSet<RemoteFile> fileQueue = new TreeSet<>(new Comparator<RemoteFile>() {
    @Override
    public int compare(RemoteFile f1, RemoteFile f2) {
      if (f1.lastModified < f2.lastModified) {
        return -1;
      } else if (f1.lastModified > f2.lastModified) {
        return 1;
      } else {
        return f1.filename.compareTo(f2.filename);
      }
    }
  });

  private URI remoteURI;
  private volatile Offset currentOffset = null;
  private InputStream currentStream = null;
  private FileObject remoteDir;
  private DataParser parser;
  private final FileSystemOptions options = new FileSystemOptions();
  private ErrorRecordHandler errorRecordHandler;

  public RemoteDownloadSource(RemoteDownloadConfigBean conf) {
    this.conf = conf;
    if (conf.knownHosts != null && !conf.knownHosts.isEmpty()) {
      this.knownHostsFile = new File(conf.knownHosts);
    } else {
      this.knownHostsFile = null;
    }
    if (conf.errorArchiveDir != null && !conf.errorArchiveDir.isEmpty()) {
      this.errorArchive = new File(conf.errorArchiveDir);
      this.moveBuffer = new byte[64 * 1024];
    } else {
      this.errorArchive = null;
      this.moveBuffer = null;
    }
  }

  @Override
  public List<ConfigIssue> init() {

    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    conf.dataFormatConfig.checkForInvalidAvroSchemaLookupMode(
        conf.dataFormat,
        "conf.dataFormatConfig",
        getContext(),
        issues
    );

    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        Groups.REMOTE.getLabel(),
        DATA_FORMAT_CONFIG_PREFIX,
        issues
    );

    try {
      this.remoteURI = new URI(conf.remoteAddress);
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(
          Groups.REMOTE.getLabel(), REMOTE_ADDRESS_CONF, Errors.REMOTE_01, conf.remoteAddress));
    }

    if (!conf.remoteAddress.startsWith("sftp") && !conf.remoteAddress.startsWith("ftp")) {
      issues.add(getContext().createConfigIssue(
          Groups.REMOTE.getLabel(), REMOTE_ADDRESS_CONF, Errors.REMOTE_15, conf.remoteAddress));
    }

    try {
      FileSystemManager fsManager = VFS.getManager();
      // If password is not specified, add the username to the URI
      switch (conf.auth) {
        case PRIVATE_KEY:
          String schemeBase = remoteURI.getScheme() + "://";
          String usernamne = resolveCredential(conf.username, "username", issues);
          remoteURI = new URI(schemeBase + usernamne + "@" + remoteURI.toString().substring(schemeBase.length()));
          File privateKeyFile = new File(conf.privateKey);
          if (!privateKeyFile.exists() || !privateKeyFile.isFile() || !privateKeyFile.canRead()) {
            issues.add(getContext().createConfigIssue(
                Groups.CREDENTIALS.getLabel(), CONF_PREFIX + "privateKey", Errors.REMOTE_10, conf.privateKey));
          } else {
            if (!remoteURI.getScheme().equals("sftp")) {
              issues.add(getContext().createConfigIssue(
                  Groups.CREDENTIALS.getLabel(), CONF_PREFIX + "privateKey", Errors.REMOTE_11));
            } else {
              SftpFileSystemConfigBuilder.getInstance().setPreferredAuthentications(options, "publickey");
              SftpFileSystemConfigBuilder.getInstance().setIdentities(options, new File[]{privateKeyFile});
              String privateKeyPassphrase = resolveCredential(
                conf.privateKeyPassphrase,
                CONF_PREFIX + "privateKeyPassphrase",
                issues
              );
              if (privateKeyPassphrase != null && !privateKeyPassphrase.isEmpty()) {
                SftpFileSystemConfigBuilder.getInstance()
                    .setUserInfo(options, new SDCUserInfo(privateKeyPassphrase));
              }
            }
          }
          break;
        case PASSWORD:
          StaticUserAuthenticator auth = new StaticUserAuthenticator(
            remoteURI.getHost(),
            resolveCredential(conf.username, "username", issues),
            resolveCredential(conf.password, "password", issues)
          );
          SftpFileSystemConfigBuilder.getInstance().setPreferredAuthentications(options, "password");
          DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(options, auth);
          break;
        default:
          break;
      }

      if("ftp".equals(remoteURI.getScheme())) {
        FtpFileSystemConfigBuilder.getInstance().setPassiveMode(options, true);
        FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(options, conf.userDirIsRoot);
        if (conf.strictHostChecking) {
          issues.add(getContext().createConfigIssue(
              Groups.CREDENTIALS.getLabel(), CONF_PREFIX + "strictHostChecking", Errors.REMOTE_12));
        }
      }

      if ("sftp".equals(remoteURI.getScheme())) {
        SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(options, conf.userDirIsRoot);
        if (conf.strictHostChecking) {
          if (knownHostsFile != null) {
            if (knownHostsFile.exists() && knownHostsFile.isFile() && knownHostsFile.canRead()) {
              SftpFileSystemConfigBuilder.getInstance().setKnownHosts(options, knownHostsFile);
              SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(options, "yes");
            } else {
              issues.add(getContext().createConfigIssue(
                  Groups.CREDENTIALS.getLabel(), CONF_PREFIX + "knownHosts", Errors.REMOTE_06, knownHostsFile));
            }

          } else {
            issues.add(getContext().createConfigIssue(
                Groups.CREDENTIALS.getLabel(), CONF_PREFIX +"strictHostChecking", Errors.REMOTE_07));
          }
        } else {
          SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(options, "no");
        }
      }

      if (issues.isEmpty()) {
        // To ensure we can connect, else we fail validation.
        remoteDir = fsManager.resolveFile(remoteURI.toString(), options);
      }

    } catch (FileSystemException | URISyntaxException ex) {
      issues.add(getContext().createConfigIssue(
          Groups.REMOTE.getLabel(), REMOTE_ADDRESS_CONF, Errors.REMOTE_08, conf.remoteAddress));
      LOG.error("Error trying to login to remote host", ex);
    }
    validateFilePattern(issues);
    if (issues.isEmpty()) {
      rateLimitElEval = FileRefUtil.createElEvalForRateLimit(getContext());;
      rateLimitElVars = getContext().createELVars();
    }
    return issues;
  }

  private String resolveCredential(CredentialValue credentialValue, String config, List<ConfigIssue> issues) {
    try {
      return credentialValue.get();
    } catch (StageException e) {
      issues.add(getContext().createConfigIssue(
        Groups.CREDENTIALS.getLabel(),
        config,
        Errors.REMOTE_17,
        e.toString()
      ));
    }

    return null;
  }

  private void validateFilePattern(List<ConfigIssue> issues) {
    if (conf.filePattern == null || conf.filePattern.trim().isEmpty()) {
      issues.add(getContext().createConfigIssue(
          Groups.REMOTE.getLabel(), CONF_PREFIX + "filePattern", Errors.REMOTE_13, conf.filePattern));
    } else {
      try {
        globToRegex(conf.filePattern);
      } catch (IllegalArgumentException ex) {
        issues.add(getContext().createConfigIssue(
            Groups.REMOTE.getLabel(), CONF_PREFIX + "filePattern", Errors.REMOTE_14, conf.filePattern, ex.toString(), ex ));
      }
    }
  }

  /**
   * Convert a limited file glob into a
   * simple regex.
   *
   * @param glob file specification glob
   * @return regex.
   */
  private String globToRegex(String glob) {
    if (glob.charAt(0) == '.' || glob.contains("/") || glob.contains("~")) {
      throw new IllegalArgumentException("Invalid character in file glob");
    }

    // treat dot as a literal.
    glob = glob.replace(".", "\\.");
    glob = glob.replace("*", ".+");
    glob = glob.replace("?", ".{1}+");
    return glob;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    final int batchSize = Math.min(maxBatchSize, conf.basic.maxBatchSize);
    // Just started up, currentOffset has not yet been set.
    // This method returns NOTHING_READ when only no events have ever been read
    if (currentOffset == null) {
      if(StringUtils.isEmpty(lastSourceOffset) || NOTHING_READ.equals(lastSourceOffset)) {
        LOG.debug("Detected invalid source offset '{}'", lastSourceOffset);

        // Use initial file
        if(!StringUtils.isEmpty(conf.initialFileToProcess)) {
          try {
            FileObject initialFile = remoteDir.resolveFile(conf.initialFileToProcess, NameScope.DESCENDENT);

            currentOffset = new Offset(
              initialFile.getName().getPath(),
              initialFile.getContent().getLastModifiedTime(),
              ZERO
            );
          } catch (FileSystemException e) {
            throw new StageException(Errors.REMOTE_16, conf.initialFileToProcess, e.toString(), e);
          }
        }

        // Otherwise start from beginning
      } else {
        // We have valid offset
        currentOffset = new Offset(lastSourceOffset);
      }
    }

    String offset = NOTHING_READ;
    try {
      Optional<RemoteFile> nextOpt = null;
      // Time to read the next file
      if (currentStream == null) {
        nextOpt = getNextFile();
        if (nextOpt.isPresent()) {
          next = nextOpt.get();
          // When starting up, reset to offset 0 of the file picked up for read only if:
          // -- we are starting up for the very first time, hence current offset is null
          // -- or the next file picked up for reads is not the same as the one we left off at (because we may have completed that one).
          if (currentOffset == null || !currentOffset.fileName.equals(next.filename)) {
            currentOffset = new Offset(next.remoteObject.getName().getPath(),
                next.remoteObject.getContent().getLastModifiedTime(), ZERO);
          }
          if (conf.dataFormat == DataFormat.WHOLE_FILE) {
            Map<String, Object> metadata = new HashMap<>(next.remoteObject.getContent().getAttributes());
            metadata.put(SIZE, next.remoteObject.getContent().getSize());
            metadata.put(LAST_MODIFIED_TIME, next.remoteObject.getContent().getLastModifiedTime());
            metadata.put(CONTENT_TYPE, next.remoteObject.getContent().getContentInfo().getContentType());
            metadata.put(CONTENT_ENCODING, next.remoteObject.getContent().getContentInfo().getContentEncoding());

            metadata.put(HeaderAttributeConstants.FILE, next.filename);
            metadata.put(HeaderAttributeConstants.FILE_NAME, FilenameUtils.getName(next.filename));
            metadata.put(REMOTE_URI, remoteURI.toString());

            FileRef fileRef = new RemoteSourceFileRef.Builder()
                .bufferSize(conf.dataFormatConfig.wholeFileMaxObjectLen)
                .totalSizeInBytes(next.remoteObject.getContent().getSize())
                .rateLimit(FileRefUtil.evaluateAndGetRateLimit(rateLimitElEval, rateLimitElVars, conf.dataFormatConfig.rateLimit))
                .remoteFile(next)
                .remoteUri(remoteURI)
                .createMetrics(true)
                .build();
            parser = conf.dataFormatConfig.getParserFactory().getParser(currentOffset.offsetStr, metadata, fileRef);
          } else {
            currentStream = next.remoteObject.getContent().getInputStream();
            LOG.info("Started reading file: " + next.filename);
            parser = conf.dataFormatConfig.getParserFactory().getParser(
                currentOffset.offsetStr, currentStream, currentOffset.offset);
          }
        } else {
          if (currentOffset == null) {
            return offset;
          } else {
            return currentOffset.offsetStr;
          }
        }
      }
      offset = addRecordsToBatch(batchSize, batchMaker, next);
    } catch (IOException | DataParserException ex) {
      // Don't retry reading this file since there can be no records produced.
      offset = MINUS_ONE;
      handleFatalException(ex, next);
    } finally {
      if (!NOTHING_READ.equals(offset) && currentOffset != null) {
        currentOffset.setOffset(offset);
      }
    }
    if (currentOffset != null) {
      return currentOffset.offsetStr;
    }
    return offset;
  }

  private String addRecordsToBatch(int maxBatchSize, BatchMaker batchMaker, RemoteFile remoteFile) throws IOException, StageException {
    String offset = NOTHING_READ;
    for (int i = 0; i < maxBatchSize; i++) {
      try {
        Record record = parser.parse();
        if (record != null) {
          record.getHeader().setAttribute(REMOTE_URI, remoteURI.toString());
          record.getHeader().setAttribute(HeaderAttributeConstants.FILE, remoteFile.filename);
          record.getHeader().setAttribute(HeaderAttributeConstants.FILE_NAME,
              FilenameUtils.getName(remoteFile.filename)
          );
          record.getHeader().setAttribute(
            HeaderAttributeConstants.LAST_MODIFIED_TIME,
            String.valueOf(remoteFile.lastModified)
          );
          record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, offset == null ? "0" : offset);
          batchMaker.addRecord(record);
          offset = parser.getOffset();
        } else {
          try {
            parser.close();
            if (currentStream != null) {
              currentStream.close();
            }
          } finally {
            parser = null;
            currentStream = null;
            next = null;
          }
          //We will return -1 for finished files (It might happen where we are the last offset and another parse
          // returns null, in that case empty batch is emitted)
          offset = MINUS_ONE;
          break;
        }
      } catch (RecoverableDataParserException ex) {
        // Propagate partially parsed record to error stream
        Record record = ex.getUnparsedRecord();
        errorRecordHandler.onError(new OnRecordErrorException(record, ex.getErrorCode(), ex.getParams()));
      } catch (ObjectLengthException ex) {
        errorRecordHandler.onError(Errors.REMOTE_02, currentOffset.fileName, offset, ex);
      }
    }
    return offset;
  }

  private void moveFileToError(RemoteFile fileToMove) {
    if (fileToMove == null) {
      LOG.warn("No file to move to error, since no file is currently in-process");
      return;
    }
    if (errorArchive != null) {
      int read;
      File errorFile = new File(errorArchive, fileToMove.filename);
      if (errorFile.exists()) {
        errorFile = new File(errorArchive, fileToMove.filename + "-" + UUID.randomUUID().toString());
        LOG.info(fileToMove.filename + " is being written out as " + errorFile.getPath() +
            " as another file of the same name exists");
      }
      try (InputStream is = fileToMove.remoteObject.getContent().getInputStream();
           OutputStream os = new BufferedOutputStream(new FileOutputStream(errorFile))) {
        while ((read = is.read(moveBuffer)) != -1) {
          os.write(moveBuffer, 0, read);
        }
      } catch (Exception ex) {
        LOG.warn("Error while trying to write out error file to " + errorFile.getName());
      }
    }
  }

  private void handleFatalException(Exception ex, RemoteFile next) throws StageException {
    if (ex instanceof FileNotFoundException) {
      LOG.warn("File: {} was found in listing, but is not downloadable", next != null ? next.filename : "(null)", ex);
    }
    if (ex instanceof ClosedByInterruptException || ex.getCause() instanceof ClosedByInterruptException) {
      //If the pipeline was stopped, we may get a ClosedByInterruptException while reading avro data.
      //This is because the thread is interrupted when the pipeline is stopped.
      //Instead of sending the file to error, publish batch and move one.
    } else {
      try {
        if (parser != null) {
          parser.close();
        }
      } catch (IOException ioe) {
        LOG.error("Error while closing parser", ioe);
      } finally {
        parser = null;
      }
      try {
        if (currentStream != null) {
          currentStream.close();
        }
      } catch (IOException ioe) {
        LOG.error("Error while closing stream", ioe);
      } finally {
        currentStream = null;
      }
      String exOffset;
      if (ex instanceof OverrunException) {
        exOffset = String.valueOf(((OverrunException) ex).getStreamOffset());
      } else {
        try {
          exOffset = (parser != null) ? parser.getOffset() : NOTHING_READ;
        } catch (IOException ex1) {
          exOffset = NOTHING_READ;
        }
      }
      switch (getContext().getOnErrorRecord()) {
        case DISCARD:
          break;
        case TO_ERROR:
          // we failed to produce a record, which leaves the input file in an unknown state.
          moveFileToError(next);
          break;
        case STOP_PIPELINE:
          if (currentOffset != null) {
            throw new StageException(Errors.REMOTE_04, currentOffset.fileName, exOffset, ex);
          } else {
            throw new StageException(Errors.REMOTE_05, ex);
          }
        default:
          throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
              getContext().getOnErrorRecord(), ex));
      }
    }
  }

  private Optional<RemoteFile> getNextFile() throws FileSystemException {
    if (fileQueue.isEmpty()) {
      queueFiles();
    }
    return Optional.fromNullable(fileQueue.pollFirst());
  }

  private void queueFiles() throws FileSystemException {
    FileSelector selector = new FileSelector() {
      @Override
      public boolean includeFile(FileSelectInfo fileInfo) throws Exception {
        return true;
      }

      @Override
      public boolean traverseDescendents(FileSelectInfo fileInfo) throws Exception {
        return conf.processSubDirectories;
      }
    };

    FileObject[] theFiles;
    // get files from current directory.
    remoteDir.refresh();
    theFiles = remoteDir.getChildren();

    if (conf.processSubDirectories) {
      // append files from subdirectories.
      theFiles = (FileObject[]) ArrayUtils.addAll(theFiles, remoteDir.findFiles(selector));
    }

    for (FileObject remoteFile : theFiles) {
      if (remoteFile.getType() != FileType.FILE) {
        continue;
      }

      //check if base name matches - not full path.
      if (!remoteFile.getName().getBaseName().matches(globToRegex(conf.filePattern))) {
        continue;
      }

      long lastModified = remoteFile.getContent().getLastModifiedTime();
      RemoteFile tempFile = new RemoteFile(remoteFile.getName().getPath(), lastModified, remoteFile);
      if (shouldQueue(tempFile)) {
        // If we are done with all files, the files with the final mtime might get re-ingested over and over.
        // So if it is the one of those, don't pull it in.
        fileQueue.add(tempFile);
      }
    }
  }

  private boolean shouldQueue(RemoteFile remoteFile) throws FileSystemException {
    // Case 1: We started up for the first time, so anything we see must be queued
    return currentOffset == null ||
        // We poll for new files only when fileQueue is empty, so we don't need to check if this file is in the queue.
        // The file can be in the fileQueue only if the file was already queued in this iteration -
        // which is not possible, since we are iterating through the children,
        // so this is the first time we are seeing the file.
        // Case 2: The file is newer than the last one we read/are reading
        ((remoteFile.lastModified > currentOffset.timestamp) ||
            // Case 3: The file has the same timestamp as the last one we read, but is lexicographically higher, and we have not queued it before.
            (remoteFile.lastModified == currentOffset.timestamp && remoteFile.filename.compareTo(currentOffset.fileName) > 0) ||
            // Case 4: It is the same file as we were reading, but we have not read the whole thing, so queue it again - recovering from a shutdown.
            remoteFile.filename.equals(currentOffset.fileName) && !currentOffset.offset.equals(MINUS_ONE));
  }

  @Override
  public void destroy() {
    LOG.info(Utils.format("Destroying {}", getInfo().getInstanceName()));
    try {
      IOUtils.closeQuietly(currentStream);
      IOUtils.closeQuietly(parser);
      if (remoteDir != null) {
        remoteDir.close();
        FileSystem fs = remoteDir.getFileSystem();
        remoteDir.getFileSystem().getFileSystemManager().closeFileSystem(fs);
      }
    } catch (IOException ex) {
      LOG.warn("Error during destroy", ex);
    } finally {
      remoteDir = null;
      //This forces the use of same RemoteDownloadSource object
      //not to have dangling reference to old stream (which is closed)
      //Also forces to initialize the next in produce call.
      currentStream = null;
      parser = null;
      currentOffset = null;
      next = null;
    }
  }

  // Offset format: Filename::timestamp::offset. I miss case classes here.
  private class Offset {
    final String fileName;
    final long timestamp;
    private String offset;
    String offsetStr;

    Offset(String offsetStr) {
      String[] parts = offsetStr.split(OFFSET_DELIMITER);
      Preconditions.checkArgument(parts.length == 3);
      this.offsetStr = offsetStr;
      this.fileName = parts[0];
      this.timestamp = Long.parseLong(parts[1]);
      this.offset = parts[2];
    }

    Offset(String fileName, long timestamp, String offset) {
      this.fileName = fileName;
      this.offset = offset;
      this.timestamp = timestamp;
      this.offsetStr = getOffsetStr();
    }

    void setOffset(String offset) {
      this.offset = offset;
      this.offsetStr = getOffsetStr();
    }

    private String getOffsetStr() {
      return fileName + OFFSET_DELIMITER + timestamp + OFFSET_DELIMITER + offset;
    }
  }

  private static class SDCUserInfo implements com.jcraft.jsch.UserInfo {

    private final String passphrase;

    SDCUserInfo(String passphrase) {
      this.passphrase = passphrase;
    }

    @Override
    public String getPassphrase() {
      return passphrase;
    }

    @Override
    public String getPassword() {
      return null;
    }

    @Override
    public boolean promptPassphrase(String message) {
      return true;
    }

    @Override
    public boolean promptYesNo(String message) {
      return false;
    }

    @Override
    public void showMessage(String message) {
    }

    @Override
    public boolean promptPassword(String message) {
      return false;
    }
  }

}
