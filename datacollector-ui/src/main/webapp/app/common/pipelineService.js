/**
 * Service for providing access to the Pipeline utility functions.
 */
angular.module('dataCollectorApp.common')
  .service('pipelineService', function(pipelineConstant, api, $q, $translate, $modal, $location, $route, _) {

    var self = this,
      translations = {},
      defaultELEditorOptions = {
        mode: {
          name: 'javascript'
        },
        inputStyle: 'contenteditable',
        showCursorWhenSelecting: true,
        lineNumbers: false,
        matchBrackets: true,
        autoCloseBrackets: '(){}\'\'""',
        cursorHeight: 1,
        extraKeys: {
          'Ctrl-Space': 'autocomplete'
        }
      };

    this.initializeDefer = undefined;

    this.init = function() {
      if(!self.initializeDefer) {
        self.initializeDefer = $q.defer();

        $q.all([
            api.pipelineAgent.getDefinitions(),
            api.pipelineAgent.getPipelines()
          ])
          .then(function (results) {
            var definitions = results[0].data,
              pipelines = results[1].data;

            //Definitions
            self.pipelineConfigDefinition = definitions.pipeline[0];
            self.stageDefintions = definitions.stages;
            self.rulesElMetadata = definitions.rulesElMetadata;

            //Pipelines
            self.pipelines = pipelines;

            self.initializeDefer.resolve();
          }, function(data) {
            self.initializeDefer.reject(data);
          });
      }

      return self.initializeDefer.promise;
    };

    /**
     * Returns Pipeline Config Definition.
     *
     * @returns {*|pipelineConfigDefinition|$scope.pipelineConfigDefinition}
     */
    this.getPipelineConfigDefinition = function() {
      return self.pipelineConfigDefinition;
    };

    /**
     * Returns Stage Definitions
     *
     * @returns {*|stageDefintions}
     */
    this.getStageDefinitions = function() {
      return self.stageDefintions;
    };

    /**
     * Returns Rules EL Metadata
     *
     * @returns {*}
     */
    this.getRulesElMetadata = function() {
      return self.rulesElMetadata;
    };

    /**
     * Returns Metric Rules EL Metadata
     */
    this.getMetricRulesElMetadata = function() {
      return {
        elConstantDefinitions: [],
        elFunctionDefinitions:[{
          name: "value",
          description: "Returns the value of the metric in context",
          group: "",
          returnType: "long",
          elFunctionArgumentDefinition: []
        },{
          name: "time:now",
          description: "Returns the current time in milliseconds.",
          group: "",
          returnType: "long",
          elFunctionArgumentDefinition: []
        }]
      };
    };

    /**
     * Returns list of Pipelines
     *
     * @returns {PipelineHomeController.pipelines|*|$scope.pipelines|a.pipelines|w.pipelines|string}
     */
    this.getPipelines = function() {
      return self.pipelines;
    };

    /**
     * Add Pipeline to pipelines list
     *
     * @param configObject
     */
    this.addPipeline = function(configObject) {
      var index = _.sortedIndex(self.pipelines, configObject.info, function(obj) {
        return obj.name.toLowerCase();
      });

      self.pipelines.splice(index, 0, configObject.info);
    };

    /**
     * Remove Pipeline from pipelines list
     *
     * @param configInfo
     */
    this.removePipeline = function(configInfo) {
      var index = _.indexOf(self.pipelines, _.find(self.pipelines, function(pipeline){
        return pipeline.name === configInfo.name;
      }));

      self.pipelines.splice(index, 1);
    };


    /**
     * Add Pipeline Config Command Handler.
     *
     */
    this.addPipelineConfigCommand = function() {
      var modalInstance = $modal.open({
        templateUrl: 'app/home/library/create/create.tpl.html',
        controller: 'CreateModalInstanceController',
        size: '',
        backdrop: 'static'
      });

      modalInstance.result.then(function (configObject) {
        self.addPipeline(configObject);
        $location.path('/collector/pipeline/' + configObject.info.name);
      }, function () {

      });
    };

    /**
     * Import link command handler
     */
    this.importPipelineConfigCommand = function(pipelineInfo, $event) {
      var modalInstance = $modal.open({
        templateUrl: 'app/home/library/import/importModal.tpl.html',
        controller: 'ImportModalInstanceController',
        size: '',
        backdrop: 'static',
        resolve: {
          pipelineInfo: function () {
            return pipelineInfo;
          }
        }
      });

      if($event) {
        $event.stopPropagation();
      }

      modalInstance.result.then(function (configObject) {
        if(configObject) {
          self.addPipeline(configObject);
          $location.path('/collector/pipeline/' + configObject.info.name);
        } else {
          $route.reload();
        }
      }, function () {

      });
    };


    /**
     * Delete Pipeline Configuration Command Handler
     */
    this.deletePipelineConfigCommand = function(pipelineInfo, $event) {
      var modalInstance = $modal.open({
        templateUrl: 'app/home/library/delete/delete.tpl.html',
        controller: 'DeleteModalInstanceController',
        size: '',
        backdrop: 'static',
        resolve: {
          pipelineInfo: function () {
            return pipelineInfo;
          }
        }
      });

      if($event) {
        $event.stopPropagation();
      }

      modalInstance.result.then(function (configInfo) {
        self.removePipeline(configInfo);
        if(self.pipelines.length) {
          $location.path('/collector/pipeline/' + self.pipelines[0].name);
        } else {
          $location.path('/');
        }
      }, function () {

      });
    };


    /**
     * Duplicate Pipeline Configuration Command Handler
     */
    this.duplicatePipelineConfigCommand = function(pipelineInfo, $event) {
      var modalInstance = $modal.open({
        templateUrl: 'app/home/library/duplicate/duplicate.tpl.html',
        controller: 'DuplicateModalInstanceController',
        size: '',
        backdrop: 'static',
        resolve: {
          pipelineInfo: function () {
            return pipelineInfo;
          }
        }
      });

      if($event) {
        $event.stopPropagation();
      }

      modalInstance.result.then(function (configObject) {
        self.addPipeline(configObject);
        $location.path('/collector/pipeline/' + configObject.info.name);
      }, function () {

      });
    };

    var getXPos = function(pipelineConfig, firstOpenLane) {
      var prevStage = (firstOpenLane && firstOpenLane.stageInstance) ? firstOpenLane.stageInstance :
        ((pipelineConfig.stages && pipelineConfig.stages.length) ? pipelineConfig.stages[pipelineConfig.stages.length - 1] : undefined);

      return prevStage ? prevStage.uiInfo.xPos + 220 : 60;
    };

    var getYPos = function(pipelineConfig, firstOpenLane, xPos) {
      var maxYPos = 0;

      if(firstOpenLane) {
        maxYPos = firstOpenLane.stageInstance.uiInfo.yPos - 150;
      }

      angular.forEach(pipelineConfig.stages, function(stage) {
        if(stage.uiInfo.xPos === xPos && stage.uiInfo.yPos > maxYPos) {
          maxYPos = stage.uiInfo.yPos;
        }
      });

      return maxYPos ? maxYPos + 150 : 50;
    };

    this.setStageDefintions = function(stageDefns) {
      self.stageDefintions = stageDefns;
    };

    /**
     * Construct new instance for give Stage Definition
     * @param options
     *  stage
     *  pipelineConfig
     *  labelSuffix [Optional]
     *  firstOpenLane [Optional]
     *  relativeXPos [Optional]
     *  relativeYPos [Optional]
     *  configuration [Optional]
     *  errorStage [optional]
     *  edges [optional]
     * @returns {{instanceName: *, library: (*|stageInstance.library|library|e.library), stageName: *, stageVersion: *, configuration: Array, uiInfo: {label: *, description: string, xPos: *, yPos: number, stageType: *}, inputLanes: Array, outputLanes: Array}}
     */
    this.getNewStageInstance = function (options) {
      var stage = options.stage,
        pipelineConfig = options.pipelineConfig,
        labelSuffix = options.labelSuffix,
        firstOpenLane = options.firstOpenLane,
        relativeXPos = options.relativeXPos,
        relativeYPos = options.relativeYPos,
        configuration = options.configuration,
        xPos = relativeXPos || getXPos(pipelineConfig, firstOpenLane),
        yPos = relativeYPos || getYPos(pipelineConfig, firstOpenLane, xPos),
        stageLabel = self.getStageLabel(stage, pipelineConfig, options),
        stageInstance = {
          instanceName: stage.name + (new Date()).getTime() + (labelSuffix ? labelSuffix : ''),
          library: stage.library,
          stageName: stage.name,
          stageVersion: stage.version,
          configuration: [],
          uiInfo: {
            label: stageLabel,
            description: '', //stage.description,
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

      if (stage.outputStreams > 0) {
        for(var i=0; i< stage.outputStreams; i++) {
          stageInstance.outputLanes.push(stageInstance.instanceName + 'OutputLane' + (new Date()).getTime() + i);
        }

        if(stage.outputStreams > 1) {
          stageInstance.uiInfo.outputStreamLabels = stage.outputStreamLabels;
        }
      } else if(stage.variableOutputStreams) {
        stageInstance.outputLanes.push(stageInstance.instanceName + 'OutputLane' + (new Date()).getTime());
      }


      if(options.insertBetweenEdge && (stage.outputStreams > 0 || stage.variableOutputStreams)) {
        //Insert stage instance in the middle of edge
        var edge = options.insertBetweenEdge,
          targetInstance = edge.target,
          laneIndex;

        stageInstance.inputLanes.push(edge.outputLane);

        angular.forEach(targetInstance.inputLanes, function(laneName, index) {
          if(laneName === edge.outputLane) {
            laneIndex = index;
          }
        });

        if(laneIndex !== undefined) {
          targetInstance.inputLanes[laneIndex] = stageInstance.outputLanes[0];
        }

        stageInstance.uiInfo.xPos = targetInstance.uiInfo.xPos - 20;
        stageInstance.uiInfo.yPos = targetInstance.uiInfo.yPos + 50;
        targetInstance.uiInfo.xPos += 200;
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

      stageInstance.uiInfo.icon = self.getStageIconURL(stage);

      if(configuration) {
        //Special handling for lanePredicates
        angular.forEach(configuration, function(config) {
          if(config.name === 'lanePredicates') {
            stageInstance.outputLanes = [];

            angular.forEach(config.value, function(lanePredicate, index) {
              var newOutputLane = stageInstance.instanceName + 'OutputLane' + (new Date()).getTime() + index;
              lanePredicate.outputLane = newOutputLane;
              stageInstance.outputLanes.push(newOutputLane);
            });
          }
        });

        stageInstance.configuration = configuration;
      }

      return stageInstance;
    };

    /**
     * Return Stage Label
     *
     * @param stage
     * @param pipelineConfig
     * @param options
     * @returns {*}
     */
    this.getStageLabel = function(stage, pipelineConfig, options) {
      var label = stage.label;

      if(options.errorStage) {
        return 'Bad Records - ' + label;
      } else {
        var similarStageInstances = _.filter(pipelineConfig.stages, function(stageInstance) {
          return stageInstance.uiInfo.label.indexOf(label) !== -1;
        });

        return label + ' ' + (similarStageInstances.length + 1);
      }
    };


    /**
     * Sets default value for config.
     *
     * @param configDefinition
     * @param stageInstance
     * @returns {{name: *, value: (defaultValue|*|string|Variable.defaultValue|undefined)}}
     */
    this.setDefaultValueForConfig = function(configDefinition, stageInstance) {
      var config = {
          name: configDefinition.name,
          value: configDefinition.defaultValue || undefined
        };

      if(configDefinition.type === 'MODEL') {
        if(configDefinition.model.modelType === 'FIELD_SELECTOR_MULTI_VALUED' && !config.value) {
          config.value = [];
        } else if(configDefinition.model.modelType === 'LANE_PREDICATE_MAPPING') {
          config.value = [{
            outputLane: stageInstance.outputLanes[0],
            predicate: 'default'
          }];
        } else if(configDefinition.model.modelType === 'COMPLEX_FIELD') {
          var complexFieldObj = {};
          angular.forEach(configDefinition.model.configDefinitions, function (complexFiledConfigDefinition) {
            var complexFieldConfig = self.setDefaultValueForConfig(complexFiledConfigDefinition, stageInstance);
            complexFieldObj[complexFieldConfig.name] = complexFieldConfig.value || '';
          });
          config.value = [complexFieldObj];
        }
      } else if(configDefinition.type === 'BOOLEAN' && config.value === undefined) {
        config.value = false;
      } else if(configDefinition.type === 'LIST' && !config.value) {
        config.value = [];
      } else if(configDefinition.type === 'MAP' && !config.value) {
        config.value = [];
      }

      return config;
    };

    /**
     * Return Stage Icon URL
     *
     * @param stage
     * @returns {string}
     */
    this.getStageIconURL = function(stage) {
      if(stage.icon) {
        return '/rest/v1/definitions/stages/icon?name=' + stage.name +
        '&library=' + stage.library + '&version=' + stage.version;
      } else {
        switch(stage.type) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            return '/assets/stage/defaultSource.svg';
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            return '/assets/stage/defaultProcessor.svg';
          case pipelineConstant.TARGET_STAGE_TYPE:
            return '/assets/stage/defaultTarget.svg';
        }
      }
    };



    /**
     * Returns message string of the issue.
     *
     * @param stageInstance
     * @param issue
     * @returns {*}
     */
    this.getIssuesMessage = function (stageInstance, issue) {
      var msg = issue.message;

      if (issue.configName && stageInstance) {
        msg += ' : ' + self.getConfigurationLabel(stageInstance, issue.configName);
      }

      return msg;
    };

    /**
     * Returns label of Configuration for given Stage Instance object and Configuration Name.
     *
     * @param stageInstance
     * @param configName
     * @returns {*}
     */
    this.getConfigurationLabel = function (stageInstance, configName) {
      var stageDefinition = _.find(self.stageDefintions, function (stage) {
          return stageInstance.library === stage.library &&
            stageInstance.stageName === stage.name &&
            stageInstance.stageVersion === stage.version;
        }),
        configDefinition = stageDefinition ? _.find(stageDefinition.configDefinitions, function (configDefinition) {
          return configDefinition.name === configName;
        }) : undefined;

      return configDefinition ? configDefinition.label : configName;
    };


    var getStageConfigurationNameConfigMap = function(stageInstance) {
      var nameConfigMap = {},
        stageDefinition = _.find(self.stageDefintions, function (stage) {
          return stageInstance.library === stage.library &&
            stageInstance.stageName === stage.name &&
            stageInstance.stageVersion === stage.version;
        });

      angular.forEach(stageDefinition.configDefinitions, function (configDefinition) {
        nameConfigMap[configDefinition.name] = configDefinition;
      });

      return nameConfigMap;
    };

    var getConfigValueString = function(configDefinition, configValue) {
      var valStr = [];
      if(configDefinition.type === 'MODEL') {
         switch(configDefinition.model.modelType) {
            case 'VALUE_CHOOSER':
              if(configDefinition.model.chooserMode === 'PROVIDED') {
                var ind = _.indexOf(configDefinition.model.values, configValue);
                return configDefinition.model.labels[ind];
              }
              break;
            case 'LANE_PREDICATE_MAPPING':
              valStr = [];
              angular.forEach(configValue, function(lanePredicate, index) {
                valStr.push({
                  Stream: index + 1,
                  Condition: lanePredicate.predicate
                });
              });
              configValue = valStr;
              break;
            case 'COMPLEX_FIELD':
              valStr = [];
              angular.forEach(configValue, function(groupValueObject) {
                var groupValStr = {};
                angular.forEach(configDefinition.model.configDefinitions, function(groupConfigDefinition) {

                  if((groupConfigDefinition.dependsOn && groupConfigDefinition.triggeredByValues) &&
                    (groupValueObject[groupConfigDefinition.dependsOn] === undefined ||
                      !_.contains(groupConfigDefinition.triggeredByValues, groupValueObject[groupConfigDefinition.dependsOn] + ''))) {
                    return;
                  }

                  groupValStr[groupConfigDefinition.label] = groupValueObject[groupConfigDefinition.name];
                });
                valStr.push(groupValStr);
              });
              configValue = valStr;
              break;
          }
      }

      if(_.isObject(configValue)) {
        return JSON.stringify(configValue);
      }

      return configValue;
    };

    /**
     * Return HTML list of Stage Configuration.
     *
     * @returns {string}
     */
    this.getStageConfigurationHTML = function(stageInstance) {
      var configurationHtml = '<ul class="config-properties">',
        nameConfigMap = getStageConfigurationNameConfigMap(stageInstance);

      angular.forEach(stageInstance.configuration, function(c) {
        var configDefinition = nameConfigMap[c.name];

        if(c.value !== undefined && c.value !== null) {

          if(configDefinition.dependsOn && configDefinition.triggeredByValues) {
            var dependsOnConfiguration = _.find(stageInstance.configuration, function(config) {
              return config.name === configDefinition.dependsOn;
            });

            if(dependsOnConfiguration.value === undefined ||
              !_.contains(configDefinition.triggeredByValues, dependsOnConfiguration.value + '')) {
              return;
            }
          }

          configurationHtml += '<li>';
          configurationHtml += '<span class="config-label">';
          configurationHtml += (configDefinition.label || configDefinition.name) + ':  ';
          configurationHtml += '</span>';
          configurationHtml += '<span class="config-value">';
          configurationHtml += getConfigValueString(configDefinition, c.value);
          configurationHtml += '</span>';
          configurationHtml += '</li>';

        }
      });

      configurationHtml += '</ul>';

      return configurationHtml;
    };

    /**
     * Returns default EL Editor Options.
     *
     * @returns Object
     */
    this.getDefaultELEditorOptions = function() {
      return defaultELEditorOptions;
    };


    /**
     * Recursively add all the field paths to list.
     *
     * @param record
     * @param fieldPaths
     * @param nonListAndMap
     */
    this.getFieldPaths = function(record, fieldPaths, nonListAndMap) {
      angular.forEach(record.value, function(value) {
        if(value.type === 'MAP' || value.type === 'LIST') {
          if(!nonListAndMap && value.path) {
            fieldPaths.push(value.path);
          }
          self.getFieldPaths(value, fieldPaths, nonListAndMap);
        } else if(value.path) {
          fieldPaths.push(value.path);
        }
      });
    };

    /**
     * Recursively add all the field paths and value to flatten map.
     *
     * @param record
     * @param flattenRecord
     */
    this.getFlattenRecord = function(record, flattenRecord) {
      angular.forEach(record.value, function(value) {
        if(value.type === 'MAP' || value.type === 'LIST') {
          self.getFlattenRecord(value, flattenRecord);
        } else if(value.path) {
          flattenRecord[value.path] = value.value;
        }
      });
    };


    /**
     * Auto Arrange the stages in the pipeline config
     *
     * @param pipelineConfig
     */
    this.autoArrange = function(pipelineConfig) {
      var xPos = 60,
        yPos = 50,
        stages = pipelineConfig.stages,
        laneYPos = {},
        laneXPos = {};

      angular.forEach(stages, function(stage) {
        var y = stage.inputLanes.length ? laneYPos[stage.inputLanes[0]]: yPos,
          x = stage.inputLanes.length ? laneXPos[stage.inputLanes[0]] + 220 : xPos;

        if(laneYPos[stage.inputLanes[0]]) {
          laneYPos[stage.inputLanes[0]] += 150;
        }

        if(!y) {
          y = yPos;
        }

        if(stage.outputLanes.length > 1) {

          angular.forEach(stage.outputLanes, function(outputLane, index) {
            laneYPos[outputLane] = y - 10 + (130 * index);
            laneXPos[outputLane] = x;
          });

          if(y === yPos) {
            y += 30 * stage.outputLanes.length;
          }

        } else {

          if(stage.outputLanes.length) {
            laneYPos[stage.outputLanes[0]] = y;
            laneXPos[stage.outputLanes[0]] = x;
          }

          if(stage.inputLanes.length > 1 && y === yPos) {
            y += 130;
          }
        }

        stage.uiInfo.xPos = x;
        stage.uiInfo.yPos = y;

        xPos = x + 220;
      });
    };

    $translate([
      //Gauge
      'metrics.CURRENT_BATCH_AGE',
      'metrics.TIME_IN_CURRENT_STAGE',
      'metrics.TIME_OF_LAST_RECEIVED_RECORD',

      //Counter
      'metrics.COUNTER_COUNT',

      //Histogram
      'metrics.HISTOGRAM_COUNT',
      'metrics.HISTOGRAM_MAX',
      'metrics.HISTOGRAM_MIN',
      'metrics.HISTOGRAM_MEAN',
      'metrics.HISTOGRAM_MEDIAN',
      'metrics.HISTOGRAM_P50',
      'metrics.HISTOGRAM_P75',
      'metrics.HISTOGRAM_P95',
      'metrics.HISTOGRAM_P98',
      'metrics.HISTOGRAM_P99',
      'metrics.HISTOGRAM_P999',
      'metrics.HISTOGRAM_STD_DEV',

      //Meters
      'metrics.METER_COUNT',
      'metrics.METER_M1_RATE',
      'metrics.METER_M5_RATE',
      'metrics.METER_M15_RATE',
      'metrics.METER_M30_RATE',
      'metrics.METER_H1_RATE',
      'metrics.METER_H6_RATE',
      'metrics.METER_H12_RATE',
      'metrics.METER_H24_RATE',
      'metrics.METER_MEAN_RATE',

      //Timer
      'metrics.TIMER_COUNT',
      'metrics.TIMER_MAX',
      'metrics.TIMER_MIN',
      'metrics.TIMER_MEAN',
      'metrics.TIMER_P50',
      'metrics.TIMER_P75',
      'metrics.TIMER_P95',
      'metrics.TIMER_P98',
      'metrics.TIMER_P99',
      'metrics.TIMER_P999',
      'metrics.TIMER_STD_DEV',
      'metrics.TIMER_M1_RATE',
      'metrics.TIMER_M5_RATE',
      'metrics.TIMER_M15_RATE',
      'metrics.TIMER_MEAN_RATE'
    ]).then(function (_translations) {
      translations = _translations;
    });

    /**
     * Returns Metric element list
     */
    this.getMetricElementList = function() {
      var elementList = {
        GAUGE: [
          {
            value: 'CURRENT_BATCH_AGE',
            label: translations['metrics.CURRENT_BATCH_AGE']
          },
          {
            value: 'TIME_IN_CURRENT_STAGE',
            label: translations['metrics.TIME_IN_CURRENT_STAGE']
          },
          {
            value: 'TIME_OF_LAST_RECEIVED_RECORD',
            label: translations['metrics.TIME_OF_LAST_RECEIVED_RECORD']
          }
        ],
        COUNTER: [
          {
            value: 'COUNTER_COUNT',
            label: translations['metrics.COUNTER_COUNT']
          }
        ],
        HISTOGRAM: [
          {
            value: 'HISTOGRAM_COUNT',
            label: translations['metrics.HISTOGRAM_COUNT']
          },
          {
            value: 'HISTOGRAM_MAX',
            label: translations['metrics.HISTOGRAM_MAX']
          },
          {
            value: 'HISTOGRAM_MIN',
            label: translations['metrics.HISTOGRAM_MIN']
          },
          {
            value: 'HISTOGRAM_MEAN',
            label: translations['metrics.HISTOGRAM_MEAN']
          },
          {
            value: 'HISTOGRAM_MEDIAN',
            label: translations['metrics.HISTOGRAM_MEDIAN']
          },
          {
            value: 'HISTOGRAM_P50',
            label: translations['metrics.HISTOGRAM_P50']
          },
          {
            value: 'HISTOGRAM_P75',
            label: translations['metrics.HISTOGRAM_P75']
          },
          {
            value: 'HISTOGRAM_P95',
            label: translations['metrics.HISTOGRAM_P95']
          },
          {
            value: 'HISTOGRAM_P98',
            label: translations['metrics.HISTOGRAM_P98']
          },
          {
            value: 'HISTOGRAM_P99',
            label: translations['metrics.HISTOGRAM_P99']
          },
          {
            value: 'HISTOGRAM_P999',
            label: translations['metrics.HISTOGRAM_P999']
          },
          {
            value: 'HISTOGRAM_STD_DEV',
            label: translations['metrics.HISTOGRAM_STD_DEV']
          }
        ],
        METER: [
          {
            value: 'METER_COUNT',
            label: translations['metrics.METER_COUNT']
          },
          {
            value: 'METER_M1_RATE',
            label: translations['metrics.METER_M1_RATE']
          },
          {
            value: 'METER_M5_RATE',
            label: translations['metrics.METER_M5_RATE']
          },
          {
            value: 'METER_M15_RATE',
            label: translations['metrics.METER_M15_RATE']
          },
          {
            value: 'METER_M30_RATE',
            label: translations['metrics.METER_M30_RATE']
          },
          {
            value: 'METER_H1_RATE',
            label: translations['metrics.METER_H1_RATE']
          },
          {
            value: 'METER_H6_RATE',
            label: translations['metrics.METER_H6_RATE']
          },
          {
            value: 'METER_H12_RATE',
            label: translations['metrics.METER_H12_RATE']
          },
          {
            value: 'METER_H24_RATE',
            label: translations['metrics.METER_H24_RATE']
          },
          {
            value: 'METER_MEAN_RATE',
            label: translations['metrics.METER_MEAN_RATE']
          }
        ],
        TIMER: [
          {
            value: 'TIMER_COUNT',
            label: translations['metrics.TIMER_COUNT']
          },
          {
            value: 'TIMER_MAX',
            label: translations['metrics.TIMER_MAX']
          },
          {
            value: 'TIMER_MEAN',
            label: translations['metrics.TIMER_MEAN']
          },
          {
            value: 'TIMER_MIN',
            label: translations['metrics.TIMER_MIN']
          },
          {
            value: 'TIMER_P50',
            label: translations['metrics.TIMER_P50']
          },
          {
            value: 'TIMER_P75',
            label: translations['metrics.TIMER_P75']
          },
          {
            value: 'TIMER_P95',
            label: translations['metrics.TIMER_P95']
          },
          {
            value: 'TIMER_P98',
            label: translations['metrics.TIMER_P98']
          },
          {
            value: 'TIMER_P99',
            label: translations['metrics.TIMER_P99']
          },
          {
            value: 'TIMER_P999',
            label: translations['metrics.TIMER_P999']
          },
          {
            value: 'TIMER_STD_DEV',
            label: translations['metrics.TIMER_STD_DEV']
          },
          {
            value: 'TIMER_M1_RATE',
            label: translations['metrics.TIMER_M1_RATE']
          },
          {
            value: 'TIMER_M5_RATE',
            label: translations['metrics.TIMER_M5_RATE']
          },
          {
            value: 'TIMER_M15_RATE',
            label: translations['metrics.TIMER_M15_RATE']
          },
          {
            value: 'TIMER_MEAN_RATE',
            label: translations['metrics.TIMER_MEAN_RATE']
          }
        ]
      };

      return elementList;
    };


    /**
     * Returns metric element list for the given pipeline.
     *
     * @param pipelineConfig
     */
    this.getMetricIDList = function(pipelineConfig) {
      var metricIDList = {
        GAUGE: [
          {
            value: 'RuntimeStatsGauge.gauge',
            label: 'Runtime Statistics Gauge'
          }
        ],
        COUNTER: [
            {
              value: 'pipeline.memoryConsumed.counter',
              label: 'Pipeline Memory Consumption Counter'
            }
        ],
        HISTOGRAM: [
            {
              value: 'pipeline.inputRecordsPerBatch.histogramM5',
              label: 'Pipeline Input Records Per Batch Histogram M5'
            },
            {
              value: 'pipeline.outputRecordsPerBatch.histogramM5',
              label: 'Pipeline Output Records Per Batch Histogram M5'
            },
            {
              value: 'pipeline.errorRecordsPerBatch.histogramM5',
              label: 'Pipeline Bad Records Per Batch Histogram M5'
            },
            {
              value: 'pipeline.errorsPerBatch.histogramM5',
              label: 'Pipeline Errors Per Batch Histogram M5'
            }
          ],
        METER: [
            {
              value: 'pipeline.batchCount.meter',
              label: 'Pipeline Batch Count Meter'
            },
            {
              value: 'pipeline.batchInputRecords.meter',
              label: 'Pipeline Batch Input Records Meter'
            },
            {
              value: 'pipeline.batchOutputRecords.meter',
              label: 'Pipeline Batch Output Records Meter '
            },
            {
              value: 'pipeline.batchErrorRecords.meter',
              label: 'Pipeline Batch Bad Records Meter'
            },
            {
              value: 'pipeline.batchErrorMessages.meter',
              label: 'Pipeline Batch Error Messages Meter'
            }
          ],
        TIMER: [{
            value: 'pipeline.batchProcessing.timer',
            label: 'Pipeline Batch Processing Timer'
          }]
        };



      angular.forEach(pipelineConfig.stages, function(stage) {
        var instanceName = stage.instanceName,
          label = stage.uiInfo.label;

        //Counters
        metricIDList.COUNTER.push.apply(metricIDList.COUNTER, [
          {
            value: 'stage.' + instanceName + '.inputRecords.counter',
            label: label + ' Input Records Counter'
          },
          {
            value: 'stage.' + instanceName + '.outputRecords.counter',
            label: label + ' Output Records Counter'
          },
          {
            value: 'stage.' + instanceName + '.errorRecords.counter',
            label: label + ' Bad Records Counter'
          },
          {
            value: 'stage.' + instanceName + '.stageErrors.counter',
            label: label + ' Stage Errors Counter'
          },
          {
            value: 'stage.' + instanceName + '.memoryConsumed.counter',
            label: label + ' Heap Memory Usage Counter (bytes)'
          }
        ]);

        //histograms
        metricIDList.HISTOGRAM.push.apply(metricIDList.HISTOGRAM, [
          {
            value: 'stage.' + instanceName + '.inputRecords.histogramM5',
            label: label + ' Input Records Histogram'
          },
          {
            value: 'stage.' + instanceName + '.outputRecords.histogramM5',
            label: label + ' Output Records Histogram'
          },
          {
            value: 'stage.' + instanceName + '.errorRecords.histogramM5',
            label: label + ' Error Records Histogram'
          },
          {
            value: 'stage.' + instanceName + '.stageErrors.histogramM5',
            label: label + ' Stage Errors Histogram'
          }
        ]);

        //meters
        metricIDList.METER.push.apply(metricIDList.METER, [
          {
            value: 'stage.' + instanceName + '.inputRecords.meter',
            label: label + ' Input Records Meter'
          },
          {
            value: 'stage.' + instanceName + '.outputRecords.meter',
            label: label + ' Output Records Meter'
          },
          {
            value: 'stage.' + instanceName + '.errorRecords.meter',
            label: label + ' Error Records Meter'
          },
          {
            value: 'stage.' + instanceName + '.stageErrors.meter',
            label: label + ' Stage Errors Meter'
          }
        ]);


        metricIDList.TIMER.push({
          value: 'stage.' + instanceName + '.batchProcessing.timer',
          label: label + ' Batch Processing Timer'
        });

      });

      return metricIDList;
    };


    /**
     * Return Pipeline and lane triggered alerts.
     *
     * @param pipelineRules
     * @param pipelineMetrics
     */
    this.getTriggeredAlerts = function(pipelineRules, pipelineMetrics) {
      var gauges = pipelineMetrics.gauges,
        alerts = [];

      angular.forEach(pipelineRules.metricsRuleDefinitions, function(rule) {
        var gaugeName = 'alert.' + rule.id + '.gauge';
        if(gauges[gaugeName]) {
          alerts.push({
            rule: rule,
            gauge: gauges[gaugeName],
            type: 'METRIC_ALERT'
          });
        }
      });

      angular.forEach(pipelineRules.dataRuleDefinitions, function(rule) {
        var gaugeName = 'alert.' + rule.id + '.gauge';
        if(gauges[gaugeName]) {
          alerts.push({
            rule: rule,
            gauge: gauges[gaugeName],
            type: 'DATA_ALERT'
          });
        }
      });

      return alerts;
    };


    this.getPredefinedMetricAlertRules = function(pipelineName) {
      return [
        {
          id: pipelineName + 'badRecords' + (new Date()).getTime(),
          alertText: "High incidence of Bad Records",
          metricId: "pipeline.batchErrorRecords.meter",
          metricType: "METER",
          metricElement: "METER_COUNT",
          condition: "${value() > 100}",
          enabled: false,
          sendEmail: false,
          valid: true
        },
        {
          id: pipelineName + 'stageErrors' + (new Date()).getTime(),
          alertText: "High incidence of Error Messages",
          metricId: "pipeline.batchErrorMessages.meter",
          metricType: "METER",
          metricElement: "METER_COUNT",
          condition: "${value() > 100}",
          enabled: false,
          sendEmail: false,
          valid: true
        },
        {
          id: pipelineName + 'idleGauge' + (new Date()).getTime(),
          alertText: "Pipeline is Idle",
          metricId: "RuntimeStatsGauge.gauge",
          metricType: "GAUGE",
          metricElement: "TIME_OF_LAST_RECEIVED_RECORD",
          condition: "${time:now() - value() > 120000}",
          sendEmail: false,
          enabled: false,
          valid: true
        },
        {
          id: pipelineName + 'batchTime' + (new Date()).getTime(),
          alertText: "Batch taking more time to process",
          metricId: "RuntimeStatsGauge.gauge",
          metricType: "GAUGE",
          metricElement: "CURRENT_BATCH_AGE",
          condition: "${value() > 200}",
          sendEmail: false,
          enabled: false,
          valid: true
        },
        {
          id: pipelineName + 'memoryLimit' + (new Date()).getTime(),
          alertText: "Memory limit for pipeline exceeded",
          metricId: "pipeline.memoryConsumed.counter",
          metricType: "COUNTER",
          metricElement: "COUNTER_COUNT",
          condition: "${value() > humanReadableToBytes('100MB')}",
          sendEmail: false,
          enabled: false,
          valid: true
        }
      ];
    };


    this.getTextELConstantDefinitions = function() {
      return [
        {
          name: "NUMBER",
          description: "Field Type Integer",
          returnType: "Type"
        },
        {
          name: "BOOLEAN",
          description: "Field Type Boolean",
          returnType: "Type"
        },
        {
          name: "BYTE",
          description: "Field Type Byte",
          returnType: "Type"
        },
        {
          name: "BYTE_ARRAY",
          description: "Field Type Byte Array",
          returnType: "Type"
        },
        {
          name: "CHAR",
          description: "Field Type Char",
          returnType: "Type"
        },
        {
          name: "DATE",
          description: "Field Type Date",
          returnType: "Type"
        },
        {
          name: "DATETIME",
          description: "Field Type Date Time",
          returnType: "Type"
        },
        {
          name: "DECIMAL",
          description: "Field Type Decimal",
          returnType: "Type"
        },
        {
          name: "DOUBLE",
          description: "Field Type Double",
          returnType: "Type"
        },
        {
          name: "FLOAT",
          description: "Field Type Float",
          returnType: "Type"
        },
        {
          name: "LIST",
          description: "Field Type List",
          returnType: "Type"
        },
        {
          name: "MAP",
          description: "Field Type Map",
          returnType: "Type"
        },
        {
          name: "LONG",
          description: "Field Type Long",
          returnType: "Type"
        },
        {
          name: "SHORT",
          description: "Field Type Short",
          returnType: "Type"
        },
        {
          name: "STRING",
          description: "Field Type String",
          returnType: "Type"
        }
      ];
    };

  });
