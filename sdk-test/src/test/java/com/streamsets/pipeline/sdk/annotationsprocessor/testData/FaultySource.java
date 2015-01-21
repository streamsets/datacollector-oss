/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.testData;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.FieldValueChooser;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;

import java.util.List;

//13. Implementation of RawSourcePreviewer must be a top level class
//14. Annotation RawSource is FaultySource which is not a Source
@GenerateResourceBundle
@RawSource(rawSourcePreviewer = TestRawSourcePreviewer.FaultyRawSourcePreviewer.class)
@StageDef(description = "Produces twitter feeds", label = "twitter_source"
  , version = "1.0")
@ConfigGroups(FaultySource.MyGroups.class)
public class FaultySource {

  //1.Faulty config should not be final
  @ConfigDef(
    defaultValue = "admin",
    label = "username",
    required = true,
    description = "The user name of the twitter user",
    type = ConfigDef.Type.STRING
  )
  public final String username;

  //2.faulty string, should not be static
  @ConfigDef(
    defaultValue = "admin",
    label = "password",
    required = true,
    description = "The password the twitter user",
    type = ConfigDef.Type.STRING
  )
  public static String password;

  //3.Faulty field, should be public
  @ConfigDef(
    defaultValue = "",
    label = "streetAddress2",
    required = true,
    description = "The domain of the twitter user",
    type = ConfigDef.Type.STRING
  )
  private String streetAddress2;

  //4. Expected either FieldSelector or FieldValueChooser annotation
  @ConfigDef(
    defaultValue = "ss",
    label = "company",
    required = true,
    description = "The domain of the twitter user",
    type = ConfigDef.Type.MODEL
  )
  public String company;

  //5. No default constructor
  public FaultySource(String username, String password) {
    this.username = username;
    this.password = password;
  }

  //6. The class neither implements an interface nor extends from a base class

  //7. The type is expected to be string but is int
  @ConfigDef(
    defaultValue = "94040",
    label = "zip",
    required = true,
    description = "The domain of the twitter user",
    type = ConfigDef.Type.STRING)
  public int zip;

  //9. Field modifier should be modeled as Map<String, String>
  @FieldValueChooser(type = ChooserMode.PROVIDED, chooserValues = MyChooserValues.class)
  @ConfigDef(
    defaultValue = "180 Sansome",
    label = "street_address",
    required = true,
    description = "The domain of the twitter user",
    type = ConfigDef.Type.MODEL)
  public String streetAddress;

  //10. Both FieldSelector and FieldValueChooser present
  @FieldSelector
  @FieldValueChooser(type = ChooserMode.PROVIDED, chooserValues = TypesProvider.class)
  @ConfigDef(
    defaultValue = "400",
    label = "ste",
    required = true,
    description = "The domain of the twitter user",
    type = ConfigDef.Type.MODEL)
  public List<String> ste;

  //11. Drop down should be modeled as 'java.lang.String'
  //12. The ConfigDef.Type for ValueChooser should be 'MODEL'
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = TypesProvider.class)
  @ConfigDef(
      defaultValue = "4",
      label = "floor",
      required = true,
      description = "The domain of the twitter user",
      type = ConfigDef.Type.STRING)
  public List<String> floor;

  //16. The type is long but the default value is string
  @ConfigDef(
      defaultValue = "Hello",
      label = "floor",
      required = true,
      description = "The domain of the twitter user",
      type = ConfigDef.Type.INTEGER)
  public long phone;

  //17. The type is int but the default value is string
  //19. Depends on "myPhone" which does not exist
  @ConfigDef(
      defaultValue = "Hello",
      label = "floor",
      required = true,
      description = "The domain of the twitter user",
      type = ConfigDef.Type.INTEGER,
      dependsOn = "myPhone",
      triggeredByValue = {"123", "567"})
  public int extension;

  //18. The type is boolean but default value is not true or false
  //20. Depends on "myExtension" which exists but is not a configuration
  //22. Group name 'X' is invalid
  @ConfigDef(
      defaultValue = "Hello",
      label = "floor",
      required = true,
      description = "The domain of the twitter user",
      type = ConfigDef.Type.BOOLEAN,
      dependsOn = "myExtension",
      triggeredByValue = {"123", "567"},
      group = "X")
  public boolean callMe;

  String myExtension;


  @ConfigDef(
    defaultValue = "admin",
    label = "username",
    required = true,
    description = "The user name of the twitter user",
    type = ConfigDef.Type.MODEL
  )
    @ComplexField
  public List<PhoneConfig> phoneConfigs;


  //23. inner class must be static
  public class PhoneConfig {

    //24. Single valued field selector must be of type String
    @ConfigDef(
      defaultValue = "Hello",
      label = "Phone",
      required = true,
      description = "The domain of the twitter user",
      type = ConfigDef.Type.MODEL)
    @FieldSelector(singleValued = true)
    public List<String> phone;

    @ConfigDef(
      defaultValue = "Hello",
      label = "Extension",
      required = true,
      description = "The domain of the twitter user",
      type = ConfigDef.Type.STRING)
    public String extn;
  }

  //15. Inner class ChooserValues must be static
  public class MyChooserValues implements ChooserValues {

    @Override
    public List<String> getValues() {
      return ImmutableList.of("a", "b");
    }

    @Override
    public List<String> getLabels() {
      return ImmutableList.of("a", "b");
    }
  }

  //21. My Groups must be enum
  public class MyGroups implements ConfigGroups.Groups {

    @Override
    public String getLabel() {
      return "a";
    }
  }

}
