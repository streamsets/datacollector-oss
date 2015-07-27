package com.streamsets.pipeline.stage.processor.fuzzy;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version = 1,
    label = "Fuzzy Field Replacer",
    description = "Canonicalizes Field Names based on fuzzy matching.",
    icon = "replacer.png"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FuzzyFieldDProcessor extends DProcessor {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field Paths",
      description = "Field paths to match against desired field names up to 1 level deep.",
      displayPosition = 10,
      group = "FUZZY"
  )
  @FieldSelector
  public List<String> rootFieldPaths;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      defaultValue = "",
      label = "Desired Field Names",
      description = "Desired output fields are matched to all input fields.",
      displayPosition = 20,
      group = "FUZZY"
  )
  public List<String> outputFieldNames;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "60",
      min = 0,
      max = 100,
      label = "Match Threshold",
      description = "Minimum match score to accept.",
      displayPosition = 30,
      group = "FUZZY"
  )
  public int matchThreshold;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Output All Candidates",
      description = "If checked, outputs all fields meeting the threshold, instead of only the best match.",
      displayPosition = 40,
      group = "FUZZY"
  )
  public boolean allCandidates;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      dependsOn = "allCandidates",
      triggeredByValue = "false",
      label = "In-place Replacement",
      description = "If checked, does not output candidate information, simply replaces the header in-place.",
      displayPosition = 50,
      group = "FUZZY"
  )
  public boolean inPlace;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Preserve Unmatched Fields",
      description = "If checked, preserves fields which did not match any desired field name.",
      displayPosition = 50,
      group = "FUZZY"
  )
  public boolean preserveUnmatchedFields;

  @Override
  protected Processor createProcessor() {
    return new FuzzyFieldProcessor(
        rootFieldPaths,
        outputFieldNames,
        matchThreshold,
        allCandidates,
        inPlace,
        preserveUnmatchedFields
    );
  }
}
