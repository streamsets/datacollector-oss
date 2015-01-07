/**
 * Service for providing access to the Pipeline utility functions.
 */
angular.module('pipelineAgentApp.common')
  .service('pipelineService', function(pipelineConstant) {

    var self = this;

    var getXPos = function(pipelineConfig, firstOpenLane) {
      var prevStage = (firstOpenLane && firstOpenLane.stageInstance) ? firstOpenLane.stageInstance :
        ((pipelineConfig.stages && pipelineConfig.stages.length) ? pipelineConfig.stages[pipelineConfig.stages.length - 1] : undefined);

      return prevStage ? prevStage.uiInfo.xPos + 300 : 200;
    };

    var getYPos = function(pipelineConfig, firstOpenLane, xPos) {
      var maxYPos = 0;
      angular.forEach(pipelineConfig.stages, function(stage) {
        if(stage.uiInfo.xPos === xPos && stage.uiInfo.yPos > maxYPos) {
          maxYPos = stage.uiInfo.yPos;
        }
      });

      return maxYPos ? maxYPos + 130 : 70;
    };

    /**
     * Construct new instance for give Stage Defintion
     * @param stage
     * @param pipelineConfig
     * @param labelSuffix [Optional]
     * @param firstOpenLane [Optional]
     * @returns {{instanceName: *, library: (*|stageInstance.library|library|e.library), stageName: *, stageVersion: *, configuration: Array, uiInfo: {label: *, description: string, xPos: *, yPos: number, stageType: *}, inputLanes: Array, outputLanes: Array}}
     */
    this.getNewStageInstance = function (stage, pipelineConfig, labelSuffix, firstOpenLane) {
      var xPos = getXPos(pipelineConfig, firstOpenLane),
        yPos = getYPos(pipelineConfig, firstOpenLane, xPos),
        stageLabel = self.getStageLabel(stage, pipelineConfig),
        stageInstance = {
          instanceName: stage.name + (new Date()).getTime() + (labelSuffix ? labelSuffix : ''),
          library: stage.library,
          stageName: stage.name,
          stageVersion: stage.version,
          configuration: [],
          uiInfo: {
            label: stageLabel,
            description: '',
            xPos: xPos,
            yPos: yPos,
            stageType: stage.type
          },
          inputLanes: [],
          outputLanes: []
        };

      if(firstOpenLane && firstOpenLane.laneName) {
        stageInstance.inputLanes.push(firstOpenLane.laneName);
      }

      if (stage.type !== pipelineConstant.TARGET_STAGE_TYPE) {
        stageInstance.outputLanes = [stageInstance.instanceName + 'OutputLane' + (new Date()).getTime()];
      }

      angular.forEach(stage.configDefinitions, function (configDefinition) {
        stageInstance.configuration.push(self.setDefaultValueForConfig(configDefinition, stageInstance));
      });

      if(stage.rawSourceDefinition && stage.rawSourceDefinition.configDefinitions) {

        stageInstance.uiInfo.rawSource = {
          configuration: []
        };

        angular.forEach(stage.rawSourceDefinition.configDefinitions, function (configDefinition) {
          stageInstance.uiInfo.rawSource.configuration.push(self.setDefaultValueForConfig(configDefinition, stageInstance));
        });
      }

      if(stage.icon) {
        stageInstance.uiInfo.icon = 'rest/v1/definitions/stages/icon?name=' + stage.name +
        '&library=' + stage.library + '&version=' + stage.version;
      } else {
        switch(stage.type) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            stageInstance.uiInfo.icon = 'assets/stage/defaultSource.svg';
            break;
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            stageInstance.uiInfo.icon = 'assets/stage/defaultProcessor.svg';
            break;
          case pipelineConstant.TARGET_STAGE_TYPE:
            stageInstance.uiInfo.icon = 'assets/stage/defaultTarget.svg';
            break;
        }
      }

      return stageInstance;
    };

    this.getStageLabel = function(stage, pipelineConfig) {
      var label = stage.label,
        similarStageInstances = _.filter(pipelineConfig.stages, function(stageInstance) {
          return stageInstance.uiInfo.label.indexOf(label) !== -1;
        });

      return label + (similarStageInstances.length + 1);
    };


    this.setDefaultValueForConfig = function(configDefinition, stageInstance) {
      var config = {
          name: configDefinition.name,
          value: configDefinition.defaultValue || undefined
        };

      if(configDefinition.type === 'MODEL') {
        if(configDefinition.model.modelType === 'FIELD_SELECTOR') {
          config.value = [];
        } else if(configDefinition.model.modelType === 'LANE_PREDICATE_MAPPING') {
          config.value = [{
            outputLane: stageInstance.outputLanes[0],
            predicate: ''
          }];
        } else if(configDefinition.model.modelType === 'COMPLEX_FIELD') {
          var complexFieldObj = {};
          angular.forEach(configDefinition.model.configDefinitions, function (complexFiledConfigDefinition) {
            var complexFieldConfig = self.setDefaultValueForConfig(complexFiledConfigDefinition, stageInstance);
            complexFieldObj[complexFieldConfig.name] = complexFieldConfig.value || '';
          });
          config.value = [complexFieldObj];
        }
      } else if(configDefinition.type === 'INTEGER') {
        if(config.value) {
          config.value = parseInt(config.value);
        } else {
          config.value = 0;
        }
      } else if(configDefinition.type === 'BOOLEAN' && config.value === undefined) {
        config.value = false;
      } else if(configDefinition.type === 'MAP') {
        config.value = [];
      }

      return config;
    };

  });