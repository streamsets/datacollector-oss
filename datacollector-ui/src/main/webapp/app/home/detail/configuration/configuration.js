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
/**
 * Controller for Configuration.
 */

angular
  .module('dataCollectorApp.home')
  .controller('ConfigurationController', function (
    $scope, $rootScope, $q, $modal, _, $timeout, api, previewService, pipelineConstant, pipelineService
  ) {

    var getIssues = function(config, issues, instanceName, serviceName, configDefinition) {
      if (instanceName && issues.stageIssues && issues.stageIssues[instanceName]) {
        issues = issues.stageIssues[instanceName];
      } else if (config.errorStage && issues.stageIssues && instanceName &&
        issues.stageIssues[config.errorStage.instanceName] &&
        issues.stageIssues[config.errorStage.instanceName] === instanceName) {
        issues = issues.stageIssues[config.errorStage.instanceName];
      } else if (config.statsAggregatorStage && issues.stageIssues && instanceName &&
        issues.stageIssues[config.statsAggregatorStage.instanceName] &&
        issues.stageIssues[config.statsAggregatorStage.instanceName] === instanceName) {
        issues = issues.stageIssues[config.statsAggregatorStage.instanceName];
      } else if (config.startEventStages[0] && issues.stageIssues && instanceName &&
        issues.stageIssues[config.startEventStages[0].instanceName] &&
        issues.stageIssues[config.startEventStages[0].instanceName] === instanceName) {
        issues = issues.stageIssues[config.startEventStages[0].instanceName];
      } else if (config.stopEventStages[0] && issues.stageIssues && instanceName &&
        issues.stageIssues[config.stopEventStages[0].instanceName] &&
        issues.stageIssues[config.stopEventStages[0].instanceName] === instanceName) {
        issues = issues.stageIssues[config.stopEventStages[0].instanceName];
      } else if (issues.pipelineIssues){
        issues = issues.pipelineIssues;
      }

      return _.filter((issues || []), function(issue) {
        return (issue.configName === configDefinition.name && issue.serviceName === serviceName);
      });
    };

    var previewBatchSizeForFetchingFieldPaths = 10;

    angular.extend($scope, {
      fieldPaths: [],
      dFieldPaths: [],
      fieldPathsType: [],
      fieldSelectorPaths: [],
      producingEventsConfig: {
        value: false
      },

      /**
       * Callback function when tab is selected.
       */
      onTabSelect: function(tab) {
        refreshCodemirrorWidget();
        switch($scope.selectedType) {
          case pipelineConstant.PIPELINE:
            $scope.selectedConfigGroupCache[$scope.pipelineConfig.info.pipelineId] = tab.name;
            break;
          case pipelineConstant.STAGE_INSTANCE:
            $scope.selectedConfigGroupCache[$scope.selectedObject.instanceName] = tab.name;
            break;
          case pipelineConstant.LINK:
            $scope.selectedConfigGroupCache[$scope.selectedObject.outputLane] = tab.name;
        }
      },

      /**
       * Returns Codemirror Options.
       *
       * @param options
       * @param configDefinition
       * @returns {*}
       */
      getCodeMirrorOptions: function(options, configDefinition) {
        var codeMirrorOptions = {};

        if (configDefinition.type !== 'TEXT') {
          codeMirrorOptions = {
            dictionary: $scope.getCodeMirrorHints(configDefinition),
            lineWrapping: $rootScope.$storage.lineWrapping
          };
        } else {
          codeMirrorOptions = {
            dictionary: $scope.getTextCodeMirrorHints(configDefinition),
            lineWrapping: $rootScope.$storage.lineWrapping
          };
        }

        return angular.extend(codeMirrorOptions, pipelineService.getDefaultELEditorOptions(), options);
      },

      /**
       * Returns EL Functions and Constants Metadata.
       *
       * @param configDefinition
       * @returns {*}
       */
      getCodeMirrorHints: function(configDefinition) {
        var pipelineConfig = $scope.pipelineConfig;
        var pipelineConstants = _.find(pipelineConfig.configuration, function (config) {
          return config.name === 'constants';
        });
        var elCatalog = pipelineService.getELCatalog();
        var elFunctionDefinitions = [];
        var elConstantDefinitions = [];

        if (configDefinition.elFunctionDefinitionsIdx) {
          angular.forEach(_.uniq(configDefinition.elFunctionDefinitionsIdx), function(idx) {
            elFunctionDefinitions.push(elCatalog.elFunctionDefinitions[parseInt(idx)]);
          });
        }

        if (configDefinition.elConstantDefinitionsIdx) {
          angular.forEach(_.uniq(configDefinition.elConstantDefinitionsIdx), function(idx) {
            elConstantDefinitions.push(elCatalog.elConstantDefinitions[parseInt(idx)]);
          });
        }

        return {
          elFunctionDefinitions: elFunctionDefinitions,
          elConstantDefinitions: elConstantDefinitions,
          pipelineConstants: pipelineConstants ? pipelineConstants.value : [],
          runtimeConfigs: pipelineService.getRuntimeConfigs(),
          regex: 'wordColonSlash'
        };
      },

      /**
       * Returns EL Constants for Text Type.
       *
       * @param configDefinition
       * @returns {*}
       */
      getTextCodeMirrorHints: function(configDefinition) {
        var hints = $scope.getCodeMirrorHints(configDefinition);

        return {
          elFunctionDefinitions: hints.elFunctionDefinitions,
          elConstantDefinitions: hints.elConstantDefinitions,
          pipelineConstants: hints.pipelineConstants,
          textMode: configDefinition.mode,
          regex: "wordColonSlashBracket"
        };
      },

      /**
       * Returns issues for the given Configuration Object and Definition.
       *
       * @param configObject
       * @param configDefinition
       */
      getConfigurationIssues: function(configObject, configDefinition) {
        var config = $scope.pipelineConfig;
        var commonErrors = $rootScope.common.errors;
        var issues;

        // The configObject can be one of two things - either StageDefinition or ServiceDefinition.
        var instanceName;
        var serviceName;
        if ("service" in configObject) {
          instanceName = $scope.detailPaneConfig.instanceName;
          serviceName = configObject.service;
        } else {
          instanceName = configObject.instanceName;
          serviceName = null;
        }

        if (config && config.issues) {
          issues = getIssues(config, config.issues, instanceName, serviceName, configDefinition);
        }

        if (issues.length === 0 && commonErrors && commonErrors.length && commonErrors[0].pipelineIssues) {
          issues = getIssues(config, commonErrors[0], instanceName, serviceName, configDefinition);
        }

        return issues;
      },

      /**
       * Toggles selection of value in given Array.
       *
       * @param arr
       * @param value
       */
      toggleSelector: function(arr, value) {
        var index = _.indexOf(arr, value);
        if (index !== -1) {
          arr.splice(index, 1);
        } else {
          arr.push(value);
        }
      },

      /**
       * Remove the field from uiInfo.inputFields and passed array.
       * @param fieldArr
       * @param index
       * @param configValueArr
       */
      removeFieldSelector: function(fieldArr, index, configValueArr) {
        var field = fieldArr[index];
        fieldArr.splice(index, 1);

        index = _.indexOf(configValueArr, field.name);
        if (index !== -1) {
          configValueArr.splice(index, 1);
        }
      },

      /**
       * Adds new field to the array uiInfo.inputFields
       * @param fieldArr
       */
      addNewField: function(fieldArr) {
        if (this.newFieldName) {
          fieldArr.push({
            name: this.newFieldName
          });
          this.newFieldName = '';
        }
      },

      /**
       * Raw Source Preview
       */
      rawSourcePreview: function() {
        api.pipelineAgent.rawSourcePreview($scope.activeConfigInfo.name, 0, $scope.detailPaneConfig.uiInfo.rawSource.configuration)
          .then(function(res) {
            $rootScope.common.errors = [];
            $scope.rawSourcePreviewData = res.data ? res.data.previewString : '';
          })
          .catch(function(res) {
            $rootScope.common.errors = [res.data];
          });
      },

      /**
       * On focus callback for field selector configuration.
       */
      onFieldSelectorFocus: function(stageInstance) {
        if ((!$scope.fieldPaths || $scope.fieldPaths.length === 0 ) && !$scope.isPipelineReadOnly &&
          !$scope.isPipelineRunning && $rootScope.$storage.runPreviewForFieldPaths) {
          updateFieldDataForStage(stageInstance);
        }
      },

      /**
       * Display Modal dialog for field selection from Preview data.
       *
       * @param config
       */
      showFieldSelectorModal: function(config) {
        var modalInstance = $modal.open({
          templateUrl: 'fieldSelectorModalContent.html',
          controller: 'FieldSelectorModalInstanceController',
          size: '',
          backdrop: true,
          resolve: {
            currentSelectedPaths: function() {
              return config.value;
            },
            activeConfigInfo: function () {
              return $scope.activeConfigInfo;
            },
            detailPaneConfig: function() {
              return $scope.detailPaneConfig;
            }
          }
        });

        modalInstance.result.then(function (selectedFieldPaths) {
          config.value = selectedFieldPaths;
        });
      },


      /**
       * Add Lane
       *
       * @param stageInstance
       * @param configValue
       */
      addLane: function(stageInstance, configValue) {
        var outputLaneName = stageInstance.instanceName + 'OutputLane' + (new Date()).getTime();
        stageInstance.outputLanes.unshift(outputLaneName);
        configValue.unshift({
          outputLane: outputLaneName,
          predicate: '${}'
        });
      },


      /**
       * Remove Lane
       *
       * @param stageInstance
       * @param configValue
       * @param lanePredicateMapping
       * @param $index
       */
      removeLane: function(stageInstance, configValue, lanePredicateMapping, $index) {
        var stages = $scope.pipelineConfig.stages;

        stageInstance.outputLanes.splice($index, 1);
        configValue.splice($index, 1);

        //Remove input lanes from stage instances
        _.each(stages, function(stage) {
          if (stage.instanceName !== stageInstance.instanceName) {
            stage.inputLanes = _.filter(stage.inputLanes, function(inputLane) {
              return inputLane !== lanePredicateMapping.outputLane;
            });
          }
        });
      },

      /**
       * Add object to List Configuration.
       *
       * @param stageInstance
       * @param configValue
       */
      addToList: function(stageInstance, configValue) {
        configValue.push('');
      },

      /**
       * Remove object from List Configuration.
       *
       * @param stageInstance
       * @param configValue
       * @param $index
       */
      removeFromList: function(stageInstance, configValue, $index) {
        configValue.splice($index, 1);
      },

      /**
       * Add object to Map Configuration.
       *
       * @param stageInstance
       * @param configValue
       */
      addToMap: function(stageInstance, configValue) {
        configValue.push({
          key: '',
          value: ''
        });
      },

      /**
       * Remove object from Map Configuration.
       *
       * @param stageInstance
       * @param configValue
       * @param mapObject
       * @param $index
       */
      removeFromMap: function(stageInstance, configValue, mapObject, $index) {
        configValue.splice($index, 1);
      },


      /**
       * Add Object to Custom Field Configuration.
       *
       * @param stageInstance
       * @param config
       * @param configDefinitions
       */
      addToCustomField: function(stageInstance, config, configDefinitions) {
        var complexFieldObj = {};
        angular.forEach(configDefinitions, function (complexFiledConfigDefinition) {
          var complexFieldConfig = pipelineService.setDefaultValueForConfig(complexFiledConfigDefinition, stageInstance);
          complexFieldObj[complexFieldConfig.name] = (complexFieldConfig.value !== undefined && complexFieldConfig.value !== null) ? complexFieldConfig.value : undefined;
        });
        if (config.value) {
          config.value.push(complexFieldObj);
        } else {
          config.value = [complexFieldObj];
        }
      },


      /**
       * Remove Object from Custom Field Configuration.
       *
       * @param stageInstance
       * @param configValue
       * @param $index
       */
      removeFromCustomField: function(stageInstance, configValue, $index) {
        configValue.splice($index, 1);
      },

      /**
       * Return Lane Index.
       *
       * @param edge
       * @returns {*}
       */
      getLaneIndex: function(edge) {
        return _.indexOf(edge.source.outputLanes, edge.outputLane) + 1;
      },

      /**
       * Returns Lane Predicate value from configuration lanePredicates.
       *
       * @param edge
       * @returns {string|config.value.predicate|predicate|d.value.predicate}
       */
      getLanePredicate: function(edge) {
        var laneIndex = _.indexOf(edge.source.outputLanes, edge.outputLane),
          lanePredicatesConfiguration = _.find(edge.source.configuration, function(configuration) {
            return configuration.name === 'lanePredicates';
          }),
          lanePredicateObject = lanePredicatesConfiguration ? lanePredicatesConfiguration.value[laneIndex] : '';
        return lanePredicateObject ? lanePredicateObject.predicate : '';
      },

      /**
       * Returns true if dependsOnMap configuration contains value in triggeredByValues.
       *
       * @param stageInstance
       * @param configDefinition
       * @returns {*}
       */
      verifyDependsOnMap: function(stageInstance, configDefinition) {
        var returnValue = true,
          valueMap = _.object(_.map(stageInstance.configuration, function(configuration) {
            return [configuration.name, configuration.value];
          }));

        angular.forEach(configDefinition.dependsOnMap, function(triggeredByValues, dependsOn) {
          var dependsOnConfigValue = valueMap[dependsOn];
          if (dependsOnConfigValue === undefined ||
            !_.contains(triggeredByValues, dependsOnConfigValue)) {
            returnValue = false;
          }
        });

        return returnValue;
      },

      /**
       * Returns true if dependsOn configuration contains value in triggeredByValues.
       *
       * @param stageInstance
       * @param configDefinition
       * @param configDefinitions
       * @returns {*}
       */
      verifyDependsOn: function(stageInstance, configDefinition, configDefinitions) {
        if (!configDefinitions) {
          configDefinitions = $scope.detailPaneConfigDefn.configDefinitions;
        }
        return $scope.verifyDependsOnMap(stageInstance, configDefinition);
      },


      /**
       * Returns true if dependsOn Custom Field configuration contains value in triggeredByValues.
       *
       * @param stageInstance
       * @param customFieldConfigValue
       * @param customConfiguration
       * @returns {*}
       */
      verifyCustomFieldDependsOn: function(stageInstance, customFieldConfigValue, customConfiguration) {
        var returnValue = true;

        angular.forEach(customConfiguration.dependsOnMap, function(triggeredByValues, dependsOn) {
          var dependsOnConfigValue = customFieldConfigValue[dependsOn];
          if (dependsOnConfigValue === undefined ||
            !_.contains(triggeredByValues, dependsOnConfigValue)) {
            returnValue = false;
          }
        });

        return returnValue;
      },

      /**
       * Returns Config Model Object
       *
       * @param stageInstance
       * @param configDefinition
       * @returns {*}
       */
      getConfigIndex: function(stageInstance, configDefinition) {
        if (stageInstance && configDefinition) {
          var configIndex;

          angular.forEach(stageInstance.configuration, function(config, index) {
            if (configDefinition.name === config.name) {
              configIndex = index;
            }
          });

          if (configIndex === undefined) {
            //No configuration found, added the configuration with default value
            stageInstance.configuration.push(pipelineService.setDefaultValueForConfig(configDefinition, stageInstance));
            configIndex = stageInstance.configuration.length - 1;
          }

          return configIndex;
        }
      },

      /**
       * Generate structure for ng-repeat of a ValueChooserModel.
       *
       * Return structure will be filtered based on the runtime value of a filterConfig if one
       * exists and is specified.
       *
       * @param instance Instance of a stage or service.
       * @param configDefinition Definition of the ValueChooser config
       * @returns [{label:*, value:*}]
       */
      getValueChooserOptions: function(instance, definition) {
        var list = [];
        var filter = $scope.getConfig(definition.model.filteringConfig, instance);

        angular.forEach(definition.model.values, function(value, index) {

          if(filter && filter.indexOf(value) < 0) {
            return;
          }

          var entry = {
            label: definition.model.labels[index],
            value: value
          };
          list.push(entry);
        });

        return list;
      },

      /**
       * Return config of given name from the stage or service instance.
       *
       * @param name Name of the config.
       * @param instance Instance of a stage or service.
       * @returns {*}
       */
      getConfig: function(name, instance) {
        var value = undefined;

        angular.forEach(instance.configuration, function(config) {
          if(config.name === name) {
            value = config.value;
          }
        });

        return value;
      },


      /**
       * Returns true if at least one config is visible in given group.
       *
       * @param stageInstance
       * @param configDefinitions
       * @param groupName
       * @returns {*}
       */
      isGroupVisible: function(stageInstance, configDefinitions, groupName) {
        var visible = false;

        angular.forEach(configDefinitions, function(configDefinition) {
          if (configDefinition.group === groupName &&
            ($scope.verifyDependsOnMap(stageInstance, configDefinition))) {
            visible = true;
          }
        });

        return visible;
      },

      /**
       * Returns true if at least one config is visible in given group. This will calculate
       * visibility in the main stage configuration and all declared services.
       *
       * @param stageInstance
       * @param stageDefinition
       * @param groupName
       * @returns {*}
       */
      isStageGroupVisible: function(stageInstance, stageDefinition, services, groupName) {
        // First see if this tab is visible in normal stage configurations
        if(this.isGroupVisible(stageInstance, stageDefinition.configDefinitions, groupName)) {
          return true;
        }

        var visible = false;
        angular.forEach(services, function(service) {
          if($scope.isGroupVisible(service.config, service.definition.configDefinitions, groupName)) {
            visible = true;
          }
        });
        return visible;
      },

      /**
       * Returns true if there is any configuration issue for given Stage Instance name and configuration group.
       *
       * @param stageInstance
       * @param groupName
       * @param errorStage
       * @returns {*}
       */
      showConfigurationWarning: function(stageInstance, groupName, errorStage) {
        var config = $scope.pipelineConfig;
        var commonErrors = $rootScope.common.errors;
        var issuesMap;
        var issues;

        if (commonErrors && commonErrors.length && commonErrors[0].pipelineIssues) {
          issuesMap = commonErrors[0];
        } else if (config && config.issues){
          issuesMap = config.issues;
        }

        if (issuesMap) {
          if (stageInstance.instanceName && issuesMap.stageIssues && issuesMap.stageIssues[stageInstance.instanceName]) {
            issues = issuesMap.stageIssues[stageInstance.instanceName];
          } else if (issuesMap.pipelineIssues && !stageInstance.instanceName) {
            issues = issuesMap.pipelineIssues;
          }
        }

        if (errorStage) {
          return issues && issues.length;
        } else {
          return _.find(issues, function(issue) {
            return issue.configGroup === groupName;
          });
        }
      },

      /**
       * Returns character value.
       *
       * @param val
       * @returns {*}
       */
      getCharacterValue: function(val) {
        if (val !== '\t' && val !== ';' && val !== ',' && val !== ' ') {
          return 'Other';
        }

        return val;
      },

      producingEventsConfigChange: function() {
        console.log($scope.producingEventsConfig);
        if ($scope.producingEventsConfig.value && $scope.detailPaneConfigDefn.producingEvents &&
          (!$scope.detailPaneConfig.eventLanes || $scope.detailPaneConfig.eventLanes.length === 0)) {
          $scope.detailPaneConfig.eventLanes = [$scope.detailPaneConfig.instanceName + '_EventLane'];
        } else if (!$scope.producingEventsConfig.value) {
          if ($scope.detailPaneConfig.eventLanes && $scope.detailPaneConfig.eventLanes.length) {
            var eventLane = $scope.detailPaneConfig.eventLanes[0];
            angular.forEach($scope.pipelineConfig.stages, function (targetStageInstance) {
              if (targetStageInstance.inputLanes && targetStageInstance.inputLanes.length) {
                targetStageInstance.inputLanes = _.filter(targetStageInstance.inputLanes, function (inputLane) {
                  return inputLane !== eventLane;
                });
              }
            });
          }
          $scope.detailPaneConfig.eventLanes = [];
        }
      }
    });

    /**
     * Refresh Codemirror widget
     */
    var refreshCodemirrorWidget = function() {
      $scope.refreshCodemirror = true;
      $timeout(function () {
        $scope.refreshCodemirror = false;
      }, 100);
    };

    /**
     * Return true if given instance looks like a Stage instance.
     */
    var isStageInstance = function(instance) {
      return !("service" in instance);
    }

    /**
     * Return true if given instance is stage instance a source.
     */
    var isSourceIstance = function(instance) {
      return isStageInstance(instance) && instance.uiInfo.stageType === pipelineConstant.SOURCE_STAGE_TYPE;
    }

    /**
     * Update Stage Preview Data when stage selection changed.
     *
     * @param stageInstance
     */
    var updateFieldDataForStage = function(stageInstance) {
      //In case of processors and targets run the preview to get input fields & if current state of config is previewable.
      if (isStageInstance(stageInstance) && !isSourceIstance(stageInstance) && !$scope.fieldPathsFetchInProgress) {
        $scope.fieldPathsFetchInProgress = true;

        $scope.fieldPaths = [];
        $scope.dFieldPaths = [];
        $scope.fieldPathsType = [];
        $scope.fieldSelectorPaths = [];

        previewService.getInputRecordsFromPreview($scope.activeConfigInfo.name, stageInstance,
          previewBatchSizeForFetchingFieldPaths).
          then(function (inputRecords) {
            $scope.fieldPathsFetchInProgress = false;
            if (_.isArray(inputRecords) && inputRecords.length) {
              var fieldPathsMap = {},
                dFieldPathsList = [];

              angular.forEach(inputRecords, function(record, index) {
                var fieldPaths = [],
                  fieldPathsType = [],
                  dFieldPaths = [];

                pipelineService.getFieldPaths(record.value, fieldPaths, false, fieldPathsType, dFieldPaths);


                angular.forEach(fieldPaths, function(fp, index) {
                  fieldPathsMap[fp] = fieldPathsType[index];
                });

                dFieldPathsList.push(dFieldPaths);
              });

              $scope.fieldPaths = _.keys(fieldPathsMap);
              $scope.fieldPathsType = _.values(fieldPathsMap);
              $scope.dFieldPaths = _.union.apply(_, dFieldPathsList);

              $scope.$broadcast('fieldPathsUpdated', $scope.fieldPaths, $scope.fieldPathsType, $scope.dFieldPaths);

              angular.forEach($scope.fieldPaths, function(fieldPath) {
                $scope.fieldSelectorPaths.push(fieldPath.replace("\\'", "\'").replace("\\\\", "\\"));
              });
            }
          },
          function(res) {
            $scope.fieldPathsFetchInProgress = false;

            // Ignore Error
            //$rootScope.common.errors = [res.data];
          });
      }
    };

    var initializeGroupInformation = function(options) {
      var groupDefn = $scope.detailPaneConfigDefn ? $scope.detailPaneConfigDefn.configGroupDefinition : undefined;

      if (groupDefn && groupDefn.groupNameToLabelMapList) {
        $scope.showGroups = (groupDefn.groupNameToLabelMapList.length > 0);

        $scope.configGroupTabs = angular.copy(groupDefn.groupNameToLabelMapList);
        // This code creates groups both for stages and general pipeline configuration. Since only
        // stages have services, we need to add their groups only selectively.
        if ('services' in $scope.detailPaneConfigDefn) {
          angular.forEach($scope.detailPaneConfigDefn.services, function(serviceDependency) {
            let serviceDef = pipelineService.getServiceDefinition(serviceDependency.service);
            angular.forEach(serviceDef.configGroupDefinition.groupNameToLabelMapList, function(item) {
              $scope.configGroupTabs.push(item);
            });
          });
        }

        // handle stats group for Pipeline
        if ($scope.selectedType === pipelineConstant.PIPELINE &&
          (!$rootScope.common.isDPMEnabled && !$scope.statsAggregatorStageConfig)) {
          $scope.configGroupTabs = _.filter($scope.configGroupTabs, function(configGroupTab) {
            return (configGroupTab.name !== 'STATS');
          });
        }

        $scope.autoFocusConfigGroup = options.configGroup;
        $scope.autoFocusConfigName = options.configName;

        if (options.configGroup) {
          angular.forEach($scope.configGroupTabs, function(groupMap) {
            if (groupMap.name === options.configGroup) {
              groupMap.active = true;
            }
          });
        }

      } else {
        $scope.showGroups = false;
        $scope.configGroupTabs = [];
      }

      if (options.configGroup && options.configGroup === 'errorStageConfig') {
        $scope.errorStageConfigActive = true;
      } else {
        $scope.errorStageConfigActive = options.errorStage;
      }

      if (options.configGroup && options.configGroup === 'statsAggregatorStageConfig') {
        $scope.statsAggregatorStageConfigActive = true;
      } else {
        $scope.statsAggregatorStageConfigActive = options.statsAggregatorStage;
      }

      if (options.configGroup && options.configGroup === 'startEventStageConfig') {
        $scope.startEventStageConfigActive = true;
      } else {
        $scope.startEventStageConfigActive = options.startEventStage;
      }

      if (options.configGroup && options.configGroup === 'stopEventStageConfig') {
        $scope.stopEventStageConfigActive = true;
      } else {
        $scope.stopEventStageConfigActive = options.stopEventStage;
      }
    };

    $scope.$on('onSelectionChange', function(event, options) {
      initializeGroupInformation(options);
      if (options.type === pipelineConstant.STAGE_INSTANCE) {
        $scope.fieldPaths = [];
        $scope.dFieldPaths = [];
        $scope.fieldPathsType = [];

        if ($scope.detailPaneConfigDefn && $scope.detailPaneConfigDefn.producingEvents) {
          $scope.producingEventsConfig.value =
            ($scope.detailPaneConfig.eventLanes && $scope.detailPaneConfig.eventLanes.length > 0);
        }
      }
    });

    if ($scope.detailPaneConfigDefn) {
      initializeGroupInformation({});
      if ($scope.detailPaneConfigDefn.producingEvents) {
        $scope.producingEventsConfig.value =
          ($scope.detailPaneConfig.eventLanes && $scope.detailPaneConfig.eventLanes.length > 0);
      }
    }

    $scope.$watch('previewMode', function() {
      refreshCodemirrorWidget();
    });
  });
