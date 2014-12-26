/**
 * Service for providing access to the Pipeline utility functions.
 */
angular.module('pipelineAgentApp.common')
  .service('pipelineService', function(pipelineConstant) {

    var self = this;

    /**
     * Construct new instance for give Stage Defintion
     * @param stage
     * @param pipelineConfig
     * @returns {{instanceName: *, library: (*|stageInstance.library|library|e.library), stageName: *, stageVersion: *, configuration: Array, uiInfo: {label: *, description: string, xPos: *, yPos: number, stageType: *}, inputLanes: Array, outputLanes: Array}}
     */
    this.getNewStageInstance = function (stage, pipelineConfig) {
      var xPos = (pipelineConfig.stages && pipelineConfig.stages.length) ?
                    pipelineConfig.stages[pipelineConfig.stages.length - 1].uiInfo.xPos + 300 : 200,
        yPos = 70,
        stageLabel = self.getStageLabel(stage, pipelineConfig),
        stageInstance = {
          instanceName: stage.name + (new Date()).getTime(),
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

      if (stage.type !== pipelineConstant.TARGET_STAGE_TYPE) {
        stageInstance.outputLanes = [stageInstance.instanceName + 'OutputLane' + (new Date()).getTime()];
      }

      angular.forEach(stage.configDefinitions, function (configDefinition) {
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

        stageInstance.configuration.push(config);
      });


      if(stage.rawSourceDefinition && stage.rawSourceDefinition.configDefinitions) {

        stageInstance.uiInfo.rawSource = {
          configuration: []
        };

        angular.forEach(stage.rawSourceDefinition.configDefinitions, function (configDefinition) {
          var config = {
            name: configDefinition.name,
            value: configDefinition.defaultValue
          };

          if(configDefinition.type === 'MODEL' && configDefinition.model.modelType === 'FIELD_SELECTOR') {
            config.value = [];
          } else if(configDefinition.type === 'INTEGER') {
            if(config.value) {
              config.value = parseInt(config.value);
            } else {
              config.value = 0;
            }
          }

          stageInstance.uiInfo.rawSource.configuration.push(config);
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

  });