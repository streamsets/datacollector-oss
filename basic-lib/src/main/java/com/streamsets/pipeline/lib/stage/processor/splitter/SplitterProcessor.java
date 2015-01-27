/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.splitter;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.StageLibError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version="1.0.0",
    label="Field Splitter",
    description = "Splits a string field into multiple strings based on the specified character separator",
    icon="splitter.png")
public class SplitterProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(SplitterProcessor.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      label = "Field to Split",
      defaultValue = "",
      description = "Record field path of the string value to split")
  public String fieldPath;

  @ConfigDef(required = true,
      type = ConfigDef.Type.CHARACTER,
      label = "Separator",
      defaultValue = " ",
      description = "The value is split using this character (use '^' for space)")
  public char separator;

  @ConfigDef(label = "Field-Paths for Splits", required = false, type = ConfigDef.Type.LIST,
      description="The list of field-paths for the resulting splits, the last field will have the rest of the string")
  public List<String> fieldPathsForSplits;

  @ConfigDef(label = "To Error If Not Enough Splits",
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "DISCARD",
      description="If there are not enough splits in the string, send the record to error")
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = OnNotEnoughSplitsChooserValues.class)
  public OnNotEnoughSplits onNotEnoughSplits;

  @ConfigDef(label = "Remove Unsplit Value",
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      description="Removes the unsplit value from the record")
  public boolean removeUnsplitValue;

  private String separatorStr;
  private String[] fieldPaths;

  @Override
  protected void init() throws StageException {
    super.init();

    if (fieldPathsForSplits.size() < 2) {
      throw new StageException(StageLibError.LIB_0700);
    }

    separatorStr = (separator == '^') ? " " : "" + separator;

    //forcing a fastpath for String.split()
    if (".$|()[{^?*+\\".contains(separatorStr)) {
      separatorStr = "\\" + separatorStr;
    }

    fieldPaths = fieldPathsForSplits.toArray(new String[fieldPathsForSplits.size()]);
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field field = record.get(fieldPath);
    String[] splits = null;
    ErrorCode error = null;
    if (field == null || field.getValue() == null) {
      error = StageLibError.LIB_0701;
    } else {
      String str = field.getValueAsString();
      splits = str.split(separatorStr, fieldPaths.length);
      if (splits.length < fieldPaths.length) {
        error = StageLibError.LIB_0702;
      }
    }
    if (error == null || onNotEnoughSplits == OnNotEnoughSplits.CONTINUE) {
      for (int i = 0; i < fieldPaths.length; i++) {
        if (splits != null && splits.length > i) {
          record.set(fieldPaths[i], Field.create(splits[i]));
        } else {
          record.set(fieldPaths[i], Field.create(Field.Type.STRING, null));
        }
      }
      if (removeUnsplitValue) {
        record.delete(fieldPath);
      }
      batchMaker.addRecord(record);
    } else {
      switch (onNotEnoughSplits) {
        case DISCARD:
          LOG.debug(error.getMessage(), record, fieldPath);
          break;
        case TO_ERROR:
          getContext().toError(record, error, record, fieldPath);
          break;
        case STOP_PIPELINE:
          throw new StageException(error, record, fieldPath);
        default:
          throw new IllegalStateException(Utils.format("It should not happen, error={}, onNotEnoughSplits={}", error,
                                                       onNotEnoughSplits));
      }
    }
  }

}
