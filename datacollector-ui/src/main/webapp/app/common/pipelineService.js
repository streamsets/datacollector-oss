/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        autoCloseBrackets: {
          pairs: '(){}\'\'""'
        },
        cursorHeight: 1,
        extraKeys: {
          'Ctrl-Space': 'autocomplete'
        }
      };

    this.initializeDefer = undefined;

    this.init = function(force) {
      if (!self.initializeDefer || (force && self.initializeDefer.promise.$$state.status !== 0)) {
        self.initializeDefer = $q.defer();

        $q.all([
            api.pipelineAgent.getDefinitions(),
            api.pipelineAgent.getPipelines()
          ])
          .then(function (results) {
            var definitions = results[0].data,
              pipelines = results[1].data,
              rulesElMetadata = definitions.rulesElMetadata,
              elFunctionDefinitions = [],
              elConstantDefinitions = [];

            //Definitions
            self.pipelineConfigDefinition = definitions.pipeline[0];
            self.stageDefinitions = definitions.stages;
            self.elCatalog = definitions.elCatalog;

            //General Rules
            angular.forEach(rulesElMetadata.general.elFunctionDefinitions, function(idx) {
              elFunctionDefinitions.push(self.elCatalog.elFunctionDefinitions[parseInt(idx)]);
            });

            angular.forEach(rulesElMetadata.general.elConstantDefinitions, function(idx) {
              elConstantDefinitions.push(self.elCatalog.elConstantDefinitions[parseInt(idx)]);
            });

            self.generalRulesElMetadata = {
              elFunctionDefinitions: elFunctionDefinitions,
              elConstantDefinitions: elConstantDefinitions,
              regex: 'wordColonSlash'
            };

            //Drift Rules
            elFunctionDefinitions = [];
            elConstantDefinitions = [];

            angular.forEach(rulesElMetadata.drift.elFunctionDefinitions, function(idx) {
              elFunctionDefinitions.push(self.elCatalog.elFunctionDefinitions[parseInt(idx)]);
            });

            angular.forEach(rulesElMetadata.drift.elConstantDefinitions, function(idx) {
              elConstantDefinitions.push(self.elCatalog.elConstantDefinitions[parseInt(idx)]);
            });

            self.driftRulesElMetadata = {
              elFunctionDefinitions: elFunctionDefinitions,
              elConstantDefinitions: elConstantDefinitions,
              regex: 'wordColonSlash'
            };

            //Alert Text Rules
            elFunctionDefinitions = [];
            elConstantDefinitions = [];

            angular.forEach(rulesElMetadata.alert.elFunctionDefinitions, function(idx) {
              elFunctionDefinitions.push(self.elCatalog.elFunctionDefinitions[parseInt(idx)]);
            });

            angular.forEach(rulesElMetadata.alert.elConstantDefinitions, function(idx) {
              elConstantDefinitions.push(self.elCatalog.elConstantDefinitions[parseInt(idx)]);
            });

            self.alertTextElMetadata = {
              elFunctionDefinitions: elFunctionDefinitions,
              elConstantDefinitions: elConstantDefinitions,
              regex: 'wordColonSlash'
            };

            //Metric Rules
            self.metricRulesELMetadata = angular.copy(self.generalRulesElMetadata);
            self.metricRulesELMetadata.elFunctionDefinitions.push({
              name: "value",
              description: "Returns the value of the metric in context",
              group: "",
              returnType: "long",
              elFunctionArgumentDefinition: []
            });

            self.metricRulesELMetadata.elFunctionDefinitions.push({
              name: "time:now",
              description: "Returns the current time in milliseconds.",
              group: "",
              returnType: "long",
              elFunctionArgumentDefinition: []
            });

            self.runtimeConfigs = definitions.runtimeConfigs;

            //Pipelines
            self.pipelines = pipelines;
            self.updatedExistingLabels();

            self.initializeDefer.resolve();
          }, function(data) {
            self.initializeDefer.reject(data);
          });
      }

      return self.initializeDefer.promise;
    };


    this.updatedExistingLabels = function() {
      // Extract current labels
      var labelMap = {};
      angular.forEach(self.pipelines, function (pipelineInfo) {
        if (pipelineInfo.metadata && pipelineInfo.metadata.labels && pipelineInfo.metadata.labels) {
          angular.forEach(pipelineInfo.metadata.labels, function (label) {
            labelMap[label] = 1;
          });
        }
      });
      self.existingPipelineLabels = _.keys(labelMap).sort();
    };

    /**
     * Refresh pipeline by re fetching from server
     */
    this.refreshPipelines = function() {
      return api.pipelineAgent.getPipelines()
        .then(function (results) {
          self.pipelines = results.data;
          self.updatedExistingLabels();
          return self.pipelines;
        });
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
     * @returns {*|stageDefinitions}
     */
    this.getStageDefinitions = function() {
      return self.stageDefinitions;
    };

    /**
     * Returns General Rules EL Metadata
     *
     * @returns {*}
     */
    this.getGeneralRulesElMetadata = function() {
      return self.generalRulesElMetadata;
    };

    /**
     * Returns Drift Rules EL Metadata
     *
     * @returns {*}
     */
    this.getDriftRulesElMetadata = function() {
      return self.driftRulesElMetadata;
    };

    /**
     * Returns Alert Text EL Metadata
     *
     * @returns {*}
     */
    this.getAlertTextElMetadata = function() {
      return self.alertTextElMetadata;
    };

    /**
     * Returns Metric Rules EL Metadata
     */
    this.getMetricRulesElMetadata = function() {
      return self.metricRulesELMetadata;
    };

    /**
     * Returns EL Catalog
     */
    this.getELCatalog = function() {
      return self.elCatalog;
    };

    /**
     * Return Runtime Config Keyset.
     *
     * @returns {*}
     */
    this.getRuntimeConfigs = function() {
      return self.runtimeConfigs;
    };


    /**
     * Returns list of Pipelines
     *
     * @returns {*}
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

      if ($event) {
        $event.stopPropagation();
      }

      modalInstance.result.then(function (configObject) {
        if (configObject) {
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
      var defer = $q.defer(),
        modalInstance = $modal.open({
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

      if ($event) {
        $event.stopPropagation();
      }

      modalInstance.result.then(function (configInfo) {
        if (_.isArray(configInfo)) {
          angular.forEach(configInfo, function(pipelineInfo) {
            self.removePipeline(pipelineInfo);
          });
        } else {
          self.removePipeline(configInfo);
        }

        defer.resolve(self.pipelines);
      }, function () {

      });

      return defer.promise;
    };


    /**
     * Duplicate Pipeline Configuration Command Handler
     */
    this.duplicatePipelineConfigCommand = function(pipelineInfo, $event) {
      var defer = $q.defer(),
        modalInstance = $modal.open({
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

      if ($event) {
        $event.stopPropagation();
      }

      modalInstance.result.then(function (newPipelineConfig) {
        if (!angular.isArray(newPipelineConfig)) {
          self.addPipeline(newPipelineConfig);
        } else {
          angular.forEach(newPipelineConfig, function(p) {
            self.addPipeline(p);
          });
        }
        defer.resolve(newPipelineConfig);
      }, function () {

      });

      return defer.promise;
    };


    /**
     * Publish Pipeline to Remote Command Handler
     */
    this.publishPipelineCommand = function(pipelineInfo, $event) {
      var defer = $q.defer(),
        modalInstance = $modal.open({
          templateUrl: 'app/home/library/publish/publishModal.tpl.html',
          controller: 'PublishModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return pipelineInfo;
            }
          }
        });

      if ($event) {
        $event.stopPropagation();
      }

      modalInstance.result.then(function (metadata) {
        defer.resolve(metadata);
      }, function () {

      });

      return defer.promise;
    };

    /**
     * Download Remote Pipeline Command Handler
     */
    this.downloadRemotePipelineConfigCommand = function($event) {
      var defer = $q.defer(),
        modalInstance = $modal.open({
          templateUrl: 'app/home/library/download_remote/downloadRemoteModal.tpl.html',
          controller: 'DownloadRemoteModalInstanceController',
          size: 'lg',
          backdrop: 'static'
        });

      if ($event) {
        $event.stopPropagation();
      }

      modalInstance.result.then(function (newPipelineConfig) {
        defer.resolve(newPipelineConfig);
      }, function () {

      });

      return defer.promise;
    };

    /**
     * Show Remote Pipeline Commit History
     * @param $event
     * @returns {*}
     */
    this.showCommitHistoryCommand = function(pipelineInfo, metadata, $event) {
      var defer = $q.defer(),
        modalInstance = $modal.open({
          templateUrl: 'app/home/library/commit_history/commitHistoryModal.tpl.html',
          controller: 'CommitHistoryModalInstanceController',
          size: 'lg',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return pipelineInfo;
            },
            metadata: function() {
              return metadata;
            }
          }
        });

      if ($event) {
        $event.stopPropagation();
      }

      modalInstance.result.then(function (updatedPipelineConfig) {
        defer.resolve(updatedPipelineConfig);
      }, function () {

      });

      return defer.promise;
    };

    var getXPos = function(pipelineConfig, firstOpenLane) {
      var prevStage = (firstOpenLane && firstOpenLane.stageInstance) ? firstOpenLane.stageInstance :
        ((pipelineConfig.stages && pipelineConfig.stages.length) ? pipelineConfig.stages[pipelineConfig.stages.length - 1] : undefined);

      return prevStage ? prevStage.uiInfo.xPos + 220 : 60;
    };

    var getYPos = function(pipelineConfig, firstOpenLane, xPos) {
      var maxYPos = 0;

      if (firstOpenLane && firstOpenLane.isEventLane) {
        maxYPos = firstOpenLane.stageInstance.uiInfo.yPos;
      } else if (firstOpenLane) {
        maxYPos = firstOpenLane.stageInstance.uiInfo.yPos - 150;
      }

      angular.forEach(pipelineConfig.stages, function(stage) {
        if (stage.uiInfo.xPos === xPos && stage.uiInfo.yPos > maxYPos) {
          maxYPos = stage.uiInfo.yPos;
        }
      });

      return maxYPos ? maxYPos + 150 : 50;
    };

    this.setStageDefinitions = function(stageDefns) {
      self.stageDefinitions = stageDefns;
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
        stageInstanceName = self.getStageInstanceName(stage, pipelineConfig, options),
        stageInstance = {
          instanceName: stageInstanceName + (labelSuffix ? labelSuffix : ''),
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
          outputLanes: [],
          eventLanes: []
        };

      if (firstOpenLane && firstOpenLane.laneName) {
        stageInstance.inputLanes.push(firstOpenLane.laneName);
      }

      if (stage.outputStreams > 0) {
        for(var i=0; i< stage.outputStreams; i++) {
          stageInstance.outputLanes.push(stageInstance.instanceName + 'OutputLane' + (new Date()).getTime() + i);
        }

        if (stage.outputStreams > 1) {
          stageInstance.uiInfo.outputStreamLabels = stage.outputStreamLabels;
        }
      } else if (stage.variableOutputStreams) {
        stageInstance.outputLanes.push(stageInstance.instanceName + 'OutputLane' + (new Date()).getTime());
      }


      if (options.insertBetweenEdge && (stage.outputStreams > 0 || stage.variableOutputStreams)) {
        //Insert stage instance in the middle of edge
        var edge = options.insertBetweenEdge,
          targetInstance = edge.target,
          laneIndex;

        stageInstance.inputLanes.push(edge.outputLane);

        angular.forEach(targetInstance.inputLanes, function(laneName, index) {
          if (laneName === edge.outputLane) {
            laneIndex = index;
          }
        });

        if (laneIndex !== undefined) {
          targetInstance.inputLanes[laneIndex] = stageInstance.outputLanes[0];
        }

        stageInstance.uiInfo.xPos = targetInstance.uiInfo.xPos - 20;
        stageInstance.uiInfo.yPos = targetInstance.uiInfo.yPos + 50;
        targetInstance.uiInfo.xPos += 200;
      }

      angular.forEach(stage.configDefinitions, function (configDefinition) {
        stageInstance.configuration.push(self.setDefaultValueForConfig(configDefinition, stageInstance));
      });

      if (stage.rawSourceDefinition && stage.rawSourceDefinition.configDefinitions) {

        stageInstance.uiInfo.rawSource = {
          configuration: []
        };

        angular.forEach(stage.rawSourceDefinition.configDefinitions, function (configDefinition) {
          stageInstance.uiInfo.rawSource.configuration.push(self.setDefaultValueForConfig(configDefinition, stageInstance));
        });
      }

      if (configuration) {
        //Special handling for lanePredicates
        angular.forEach(configuration, function(config) {
          if (config.name === 'lanePredicates') {
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

      if (options.errorStage) {
        return 'Error Records - ' + label;
      } else if (options.statsAggregatorStage) {
        return 'Stats Aggregator - ' + label;
      } else {
        var similarStageInstances = _.filter(pipelineConfig.stages, function(stageInstance) {
          return stageInstance.uiInfo.label.indexOf(label) !== -1;
        });

        return label + ' ' + (similarStageInstances.length + 1);
      }
    };

    /**
     * Return Stage Instance Name
     *
     * @param stage
     * @param pipelineConfig
     * @param options
     * @returns {*}
     */
    this.getStageInstanceName = function(stage, pipelineConfig, options) {
      var stageName = stage.label.replace(/ /g, '');

      if (options.errorStage) {
        return stageName + '_ErrorStage';
      } else if (options.statsAggregatorStage) {
        return stageName + '_StatsAggregatorStage';
      } else {
        var similarStageInstancesNumber = [];
        angular.forEach(pipelineConfig.stages, function(stageInstance) {
          if (stageInstance.instanceName.indexOf(stageName) !== -1) {
            var instanceNameArr = stageInstance.instanceName.split('_');
            if (!isNaN(instanceNameArr[instanceNameArr.length - 1])) {
              var stageNumber = instanceNameArr[instanceNameArr.length - 1];
              if (stageNumber !== undefined && stageNumber.length > 1) {
                similarStageInstancesNumber.push(stageNumber);
              } else {
                similarStageInstancesNumber.push('0' + stageNumber);
              }
            }
          }
        });

        if (similarStageInstancesNumber.length) {
          similarStageInstancesNumber.sort();
          var newNumber = parseInt(similarStageInstancesNumber[similarStageInstancesNumber.length - 1]) + 1;
          if (newNumber < 10) {
            newNumber = '0' + newNumber;
          }
          return stageName + '_' +  newNumber;
        } else {
          return stageName + '_01';
        }
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
          value: (configDefinition.defaultValue !== undefined && configDefinition.defaultValue !== null) ? configDefinition.defaultValue : undefined
        };

      if (configDefinition.type === 'MODEL') {
        if (configDefinition.model.modelType === 'FIELD_SELECTOR_MULTI_VALUE' && !config.value) {
          config.value = [];
        } else if (configDefinition.model.modelType === 'PREDICATE') {
          config.value = [{
            outputLane: stageInstance.outputLanes[0],
            predicate: 'default'
          }];
        } else if (configDefinition.model.modelType === 'LIST_BEAN') {
          var complexFieldObj = {};
          angular.forEach(configDefinition.model.configDefinitions, function (complexFiledConfigDefinition) {
            var complexFieldConfig = self.setDefaultValueForConfig(complexFiledConfigDefinition, stageInstance);
            complexFieldObj[complexFieldConfig.name] = (complexFieldConfig.value !== undefined && complexFieldConfig.value !== null) ? complexFieldConfig.value : undefined;
          });
          config.value = [complexFieldObj];
        }
      } else if (configDefinition.type === 'BOOLEAN' && config.value === undefined) {
        config.value = false;
      } else if (configDefinition.type === 'LIST' && !config.value) {
        config.value = [];
      } else if (configDefinition.type === 'MAP' && !config.value) {
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
      if (stage.icon) {
        return 'rest/' + api.apiVersion + '/definitions/stages/' + stage.library + '/' + stage.name + '/icon';
      } else {
        switch(stage.type) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            return 'assets/stage/defaultSource.svg';
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            return 'assets/stage/defaultProcessor.svg';
          case pipelineConstant.TARGET_STAGE_TYPE:
            return 'assets/stage/defaultTarget.svg';
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
      var stageDefinition = _.find(self.stageDefinitions, function (stage) {
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
        stageDefinition = _.find(self.stageDefinitions, function (stage) {
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
      if (configDefinition.type === 'MODEL') {
         switch(configDefinition.model.modelType) {
            case 'VALUE_CHOOSER':
              if (configDefinition.model.chooserMode === 'PROVIDED') {
                var ind = _.indexOf(configDefinition.model.values, configValue);
                return configDefinition.model.labels[ind];
              }
              break;
            case 'PREDICATE':
              valStr = [];
              angular.forEach(configValue, function(lanePredicate, index) {
                valStr.push({
                  Stream: index + 1,
                  Condition: lanePredicate.predicate
                });
              });
              configValue = valStr;
              break;
            case 'LIST_BEAN':
              valStr = [];
              angular.forEach(configValue, function(groupValueObject) {
                var groupValStr = {};
                angular.forEach(configDefinition.model.configDefinitions, function(groupConfigDefinition) {

                  if ((groupConfigDefinition.dependsOn && groupConfigDefinition.triggeredByValues) &&
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

      if (_.isObject(configValue)) {
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

        if (c.value !== undefined && c.value !== null) {

          if (configDefinition.dependsOn && configDefinition.triggeredByValues) {
            var dependsOnConfiguration = _.find(stageInstance.configuration, function(config) {
              return config.name === configDefinition.dependsOn;
            });

            if (dependsOnConfiguration.value === undefined ||
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
     * Returns true if record is CSV with header information.
     */
    this.isCSVRecord = function(record) {
      return record && record.type === 'LIST' && record.value.length > 0 && record.value[0].type === 'MAP' &&
        record.value[0].value.header;
    };

    /**
     * Recursively add all the field paths to list.
     *
     * @param record
     * @param fieldPaths
     * @param nonListAndMap
     * @param fieldPathsType
     * @param dFieldPaths
     */
    this.getFieldPaths = function(record, fieldPaths, nonListAndMap, fieldPathsType, dFieldPaths) {
      if (!dFieldPaths) {
        dFieldPaths = [];
      }
      var keys;
      if (record.type === 'LIST') {
        angular.forEach(record.value, function(value) {
          if (value.type === 'MAP' || value.type === 'LIST' || value.type === 'LIST_MAP') {
            if (!nonListAndMap && value.sqpath) {
              fieldPaths.push(value.sqpath);
              dFieldPaths.push(value.dqpath);

              if (fieldPathsType) {
                fieldPathsType.push(value.type);
              }
            }
            self.getFieldPaths(value, fieldPaths, nonListAndMap, fieldPathsType, dFieldPaths);
          } else if (value.sqpath) {
            fieldPaths.push(value.sqpath);
            dFieldPaths.push(value.dqpath);

            if (fieldPathsType) {
              fieldPathsType.push(value.type);
            }
          }
        });
      } else if (record.type === 'MAP') {
        if (record.value) {
          keys = Object.keys(record.value).sort();
          angular.forEach(keys, function(key) {
            var value = record.value[key];
            if (value.type === 'MAP' || value.type === 'LIST' || value.type === 'LIST_MAP') {
              if (!nonListAndMap && value.sqpath) {
                fieldPaths.push(value.sqpath);
                dFieldPaths.push(value.dqpath);

                if (fieldPathsType) {
                  fieldPathsType.push(value.type);
                }
              }
              self.getFieldPaths(value, fieldPaths, nonListAndMap, fieldPathsType, dFieldPaths);
            } else if (value.sqpath) {
              fieldPaths.push(value.sqpath);
              dFieldPaths.push(value.dqpath);

              if (fieldPathsType) {
                fieldPathsType.push(value.type);
              }
            }
          });
        }
      } else if (record.type === 'LIST_MAP') {
        angular.forEach(record.value, function(value, index) {
          if (value.type === 'MAP' || value.type === 'LIST' || value.type === 'LIST_MAP') {
            if (!nonListAndMap && value.sqpath) {
              fieldPaths.push(value.sqpath);
              dFieldPaths.push(value.dqpath);

              if (fieldPathsType) {
                fieldPathsType.push(value.type);
              }
            }
            self.getFieldPaths(value, fieldPaths, nonListAndMap, fieldPathsType, dFieldPaths);
          } else if (value.sqpath) {
            fieldPaths.push(value.sqpath);
            dFieldPaths.push(value.dqpath);

            if (!nonListAndMap) {
              fieldPaths.push('[' + index + ']');
              dFieldPaths.push('[' + index + ']');
            }

            if (fieldPathsType) {
              fieldPathsType.push(value.type);
            }
          }
        });
      } else {
        fieldPaths.push(pipelineConstant.NON_LIST_MAP_ROOT);
        dFieldPaths.push(pipelineConstant.NON_LIST_MAP_ROOT);

        if (fieldPathsType) {
          fieldPathsType.push(record.type);
        }
      }

    };

    /**
     * Special handling for CSV Record.
     * Recursively add all the field paths to list.
     *
     * @param record
     * @param fieldPaths
     */
    this.getFieldPathsForCSVRecord = function(record, fieldPaths) {
      angular.forEach(record.value, function(value) {
        fieldPaths.push(value.value.header.value);
      });
    };


    /**
     * Recursively add all the field paths and value to flatten map.
     *
     * @param record
     * @param flattenRecord
     */
    this.getFlattenRecord = function(record, flattenRecord) {
      if (record.type === 'MAP' || record.type === 'LIST' || record.type === 'LIST_MAP') {
        angular.forEach(record.value, function(value) {
          if (value.type === 'MAP' || value.type === 'LIST' || value.type === 'LIST_MAP') {
            self.getFlattenRecord(value, flattenRecord);
          } else if (value.sqpath) {
            flattenRecord[value.sqpath] = value;
          }
        });
      } else {
        flattenRecord[pipelineConstant.NON_LIST_MAP_ROOT] = record;
      }
    };


    /**
     * Recursively add all the field paths and value to flatten map.
     * Special handling for CSV Record.
     *
     * @param record
     * @param flattenRecord
     */
    this.getFlattenRecordForCSVRecord = function(record, flattenRecord) {
      angular.forEach(record.value, function(value) {
        flattenRecord[value.value.header.value] = value.value.value;
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

        // handle stages with multiple inputs
        if (stage.inputLanes.length > 1) {
          var mX = 0;
          angular.forEach(stage.inputLanes, function(inputLane)  {
            if (laneXPos[inputLane] > mX) {
              mX = laneXPos[inputLane];
            }
          });
          x = mX + 220;
        }

        if (laneYPos[stage.inputLanes[0]]) {
          laneYPos[stage.inputLanes[0]] += 150;
        }

        if (!y) {
          y = yPos;
        }

        if (stage.outputLanes.length > 1) {

          angular.forEach(stage.outputLanes, function(outputLane, index) {
            laneYPos[outputLane] = y - 10 + (130 * index);
            laneXPos[outputLane] = x;
          });

          if (y === yPos) {
            y += 30 * stage.outputLanes.length;
          }

        } else {

          if (stage.outputLanes.length) {
            laneYPos[stage.outputLanes[0]] = y;
            laneXPos[stage.outputLanes[0]] = x;
          }

          if (stage.inputLanes.length > 1 && y === yPos) {
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
              label: 'Pipeline Memory Consumption Counter (MB)'
            },
            {
              value: 'pipeline.batchCount.counter',
              label: 'Pipeline Batch Counter'
            },
            {
              value: 'pipeline.batchInputRecords.counter',
              label: 'Pipeline Batch Input Records Counter'
            },
            {
              value: 'pipeline.batchOutputRecords.counter',
              label: 'Pipeline Batch Output Records Counter '
            },
            {
              value: 'pipeline.batchErrorRecords.counter',
              label: 'Pipeline Batch Error Records Counter'
            },
            {
              value: 'pipeline.batchErrorMessages.counter',
              label: 'Pipeline Batch Stage Errors Counter'
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
              label: 'Pipeline Batch Error Records Meter'
            },
            {
              value: 'pipeline.batchErrorMessages.meter',
              label: 'Pipeline Batch Stage Errors Meter'
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
            value: 'stage.' + instanceName + '.memoryConsumed.counter',
            label: label + ' Heap Memory Usage Counter (MB)'
          },
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
            label: label + ' Error Records Counter'
          },
          {
            value: 'stage.' + instanceName + '.stageErrors.counter',
            label: label + ' Stage Errors Counter'
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
     * @param pipelineName
     * @param pipelineRules
     * @param pipelineMetrics
     */
    this.getTriggeredAlerts = function(pipelineName, pipelineRules, pipelineMetrics) {
      if (!pipelineMetrics || !pipelineMetrics.gauges) {
        return;
      }

      var gauges = pipelineMetrics.gauges,
        alerts = [];

      angular.forEach(pipelineRules.metricsRuleDefinitions, function(rule) {
        var gaugeName = 'alert.' + rule.id + '.gauge';
        if (gauges[gaugeName]) {
          alerts.push({
            pipelineName: pipelineName,
            ruleDefinition: rule,
            gauge: gauges[gaugeName],
            type: 'METRIC_ALERT'
          });
        }
      });

      angular.forEach(pipelineRules.dataRuleDefinitions, function(rule) {
        var gaugeName = 'alert.' + rule.id + '.gauge';
        if (gauges[gaugeName]) {
          alerts.push({
            pipelineName: pipelineName,
            ruleDefinition: rule,
            gauge: gauges[gaugeName],
            type: 'DATA_ALERT'
          });
        }
      });

      angular.forEach(pipelineRules.driftRuleDefinitions, function(rule) {
        var gaugeName = 'alert.' + rule.id + '.gauge';
        if (gauges[gaugeName]) {
          alerts.push({
            pipelineName: pipelineName,
            ruleDefinition: rule,
            gauge: gauges[gaugeName],
            type: 'DATA_DRIFT_ALERT'
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
          condition: "${value() > (jvm:maxMemoryMB() * 0.65)}",
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
          name: "TIME",
          description: "Field Type Time",
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
