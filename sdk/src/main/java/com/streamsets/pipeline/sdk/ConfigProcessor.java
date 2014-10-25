package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageDef;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportedAnnotationTypes({"com.streamsets.pipeline.api.StageDef"})
@SupportedSourceVersion(SourceVersion.RELEASE_7)
public class ConfigProcessor extends AbstractProcessor {

  /*An instance of StageCollection collects all the stage definitions and configurations
  in maps and will later be serialized into json.*/
  private StageCollection stageCollection = null;

  /*The compiler may call this annotation processor multiple times in different rounds.
  We just need to process and generate only once*/
  private boolean generated = false;
  /*Captures if there is an error*/
  private boolean error = false;

  public ConfigProcessor() {
    super();
    stageCollection = new StageCollection();
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {

    //get each class annotated with the StageDef annotation
    for(Element e : roundEnv.getElementsAnnotatedWith(StageDef.class)) {

      ElementKind eKind = e.getKind();
      //It will most likely be a class. being extra safe
      if(eKind.isClass()) {
        TypeElement typeElement = (TypeElement) e;

        //Process teh class with StageDef annotation
        StageDef stageDefAnnotation = e.getAnnotation(StageDef.class);

        StageConfiguration stageConfiguration = new StageConfiguration();
        stageConfiguration.getStageOptions().put("name", stageDefAnnotation.name());
        stageConfiguration.getStageOptions().put("version", stageDefAnnotation.version());
        stageConfiguration.getStageOptions().put("label", stageDefAnnotation.label());
        stageConfiguration.getStageOptions().put("description", stageDefAnnotation.description());
        if(getTypeFromElement(typeElement).isEmpty()) {
          //Stage does not implement one of the Stage interface or extend the base stage class
          //This must be flagged as a compiler error.
          processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
            "Stage " + e.getSimpleName()
              + " neither extends one of BaseSource, BaseProcessor, BaseTarget classes nor implements one of Source, Processor, Target interface.");
          //Continue for now to find out if there are more issues.
          error = true;
          continue;
        }
        stageConfiguration.getStageOptions().put("type", getTypeFromElement(typeElement));

        //Process all fields with ConfigDef annotation
        List<? extends Element> enclosedElements = typeElement.getEnclosedElements();
        List<VariableElement> variableElements = ElementFilter.fieldsIn(enclosedElements);
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

    //generate a json file for the StageCollection object
    try {
      if(!generated && !error) {
        FileObject resource = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT, "",
          "PipelineStages.json", (Element[])null);
        SerializationUtil.serialize(stageCollection, resource.openOutputStream());
        generated = true;
      }
    } catch (IOException e) {
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getLocalizedMessage());
    }

    return (true);
  }

  private String getTypeFromElement(TypeElement typeElement) {

    String result = null;

    //Check if the stage extends one of the abstract classes
    TypeMirror typeMirror = typeElement.getSuperclass();
    if(typeMirror != null) {
      if (typeMirror.toString().contains("BaseSource")) {
        result = "SOURCE";
      } else if (typeMirror.toString().contains("BaseProcessor")) {
        result = "PROCESSOR";
      } else if (typeMirror.toString().contains("BaseTarget")) {
        result = "TARGET";
      }
    }

    if(result != null) {
      return result;
    }

    //if not, check if it implements one of the interfaces
    List<? extends TypeMirror> interfaces = typeElement.getInterfaces();

    if(interfaces.size() != 0) {
      if (interfaces.get(0).toString().contains("Source")) {
        result = "SOURCE";
      } else if (interfaces.get(0).toString().contains("Processor")) {
        result = "PROCESSOR";
      } else if (interfaces.get(0).toString().contains("Target")) {
        result = "TARGET";
      }
    }

    if(result != null) {
      return result;
    }

    return "";
  }
}