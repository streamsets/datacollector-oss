package com.external.stage;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.util.*;

@SupportedAnnotationTypes({"com.external.stage.StageDef"})
@SupportedSourceVersion(SourceVersion.RELEASE_7)
public class ConfigProcessor extends AbstractProcessor {

  private StageCollection stageCollection = null;
  private boolean generated = false;

  public ConfigProcessor() {
    super();
    stageCollection = new StageCollection();
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {

   //get each class annotated with the StageDef annotation
    for(Element e : roundEnv.getElementsAnnotatedWith(StageDef.class)) {

      ElementKind eKind = e.getKind();
      //It will most likely be a class. Just to avoid user error
      if(eKind.isClass()) {
        TypeElement typeElement = (TypeElement) e;
        List<? extends Element> enclosedElements = typeElement.getEnclosedElements();
        List<VariableElement> variableElements = ElementFilter.fieldsIn(enclosedElements);

        //populate StageDef annotation literals
        StageDef stageDefAnnot = e.getAnnotation(StageDef.class);
        StageConfiguration stageConfiguration = new StageConfiguration();
        stageConfiguration.getStageOptions().put("name", stageDefAnnot.name());
        stageConfiguration.getStageOptions().put("version", stageDefAnnot.version());
        stageConfiguration.getStageOptions().put("label", stageDefAnnot.label());
        stageConfiguration.getStageOptions().put("description", stageDefAnnot.description());
        stageConfiguration.getStageOptions().put("type", getTypeFromElement(typeElement));

        for (VariableElement variableElement : variableElements) {
          ConfigDef configDefAnnot = variableElement.getAnnotation(ConfigDef.class);
          if(configDefAnnot != null) {
            Map<String, String> option = new HashMap<String, String>();
            option.put("name", configDefAnnot.name());
            option.put("type", configDefAnnot.type().name());
            option.put("label", configDefAnnot.label());
            option.put("description", configDefAnnot.description());
            option.put("defaultValue", configDefAnnot.defaultValue());
            option.put("required", String.valueOf(configDefAnnot.required()));

            stageConfiguration.getConfigOptions().add(option);
          }
        }
        stageCollection.getStageConfigurations().add(stageConfiguration);
      }
    }

    //Dump contents into a file
    try {
      if(!generated) {
        FileObject resource = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT, "", "stageCollection.json", (Element[])null);
        SerializationUtil.serialize(stageCollection, resource.openOutputStream());
        generated = true;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return (true);
  }

  private String getTypeFromElement(TypeElement typeElement) {

    List<? extends TypeMirror> interfaces = typeElement.getInterfaces();
    assert interfaces.size() == 1;
    processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, interfaces.get(0).toString());
    if(interfaces.get(0).toString().contains("Source")) {
      return "SOURCE";
    } else if (interfaces.get(0).toString().contains("Processor")) {
      return "PROCESSOR";
    } else if (interfaces.get(0).toString().contains("Target")) {
      return "TARGET";
    }

    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
      "The Stage must implement one and only one of the following interfaces: Source, Processor, Target");
    return "";
  }
}