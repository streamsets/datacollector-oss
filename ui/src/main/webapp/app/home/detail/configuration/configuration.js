/**
 * Controller for Configuration.
 */

angular
  .module('dataCollectorApp.home')

  .controller('ConfigurationController', function ($scope, $rootScope, $q, $modal, _,
                                                   api, previewService, pipelineConstant, pipelineService) {
    var fieldsPathList;

    var getIssueMessage = function(config, issues, instanceName, configDefinition) {
      if(instanceName && issues.stageIssues && issues.stageIssues[instanceName]) {
        issues = issues.stageIssues[instanceName];
      } else if(config.errorStage && issues.stageIssues && issues.stageIssues[config.errorStage.instanceName]) {
        issues = issues.stageIssues[config.errorStage.instanceName];
      } else if(issues.pipelineIssues){
        issues = issues.pipelineIssues;
      }

      var filteredIssues = _.filter(issues, function(issue) {
        return (issue.configName === configDefinition.name);
      });

      return filteredIssues && filteredIssues.length ? _.pluck(filteredIssues, 'message').join(' , ') : '';
    };

    angular.extend($scope, {
      fieldPaths: [],

      /**
       * Returns message for the give Configuration Object and Definition.
       *
       * @param configObject
       * @param configDefinition
       */
      getConfigurationIssueMessage: function(configObject, configDefinition) {
        var config = $scope.pipelineConfig,
          commonErrors = $rootScope.common.errors,
          issue;

        if(config && config.issues) {
          issue = getIssueMessage(config, config.issues, configObject.instanceName, configDefinition);
        }

        if(!issue && commonErrors && commonErrors.length && commonErrors[0].pipelineIssues) {
          issue = getIssueMessage(config, commonErrors[0], configObject.instanceName, configDefinition);
        }

        return issue;
      },

      /**
       * Toggles selection of value in given Array.
       *
       * @param arr
       * @param value
       */
      toggleSelector: function(arr, value) {
        var index = _.indexOf(arr, value);
        if(index !== -1) {
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
        if(index !== -1) {
          configValueArr.splice(index, 1);
        }
      },

      /**
       * Adds new field to the array uiInfo.inputFields
       * @param fieldArr
       */
      addNewField: function(fieldArr) {
        if(this.newFieldName) {
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
          .success(function(data) {
            $rootScope.common.errors = [];
            $scope.rawSourcePreviewData = data ? data.previewString : '';
          })
          .error(function(data, status, headers, config) {
            $rootScope.common.errors = [data];
          });
      },


      /**
       * On focus callback for field selector configuration.
       */
      onFieldSelectorFocus: function(stageInstance) {
        if(!$scope.fieldPaths || $scope.fieldPaths.length === 0 ) {
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
          predicate: ''
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
          if(stage.instanceName !== stageInstance.instanceName) {
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
       * @param configValue
       * @param configDefinitions
       */
      addToCustomField: function(stageInstance, configValue, configDefinitions) {
        var complexFieldObj = {};
        angular.forEach(configDefinitions, function (complexFiledConfigDefinition) {
          var complexFieldConfig = pipelineService.setDefaultValueForConfig(complexFiledConfigDefinition, stageInstance);
          complexFieldObj[complexFieldConfig.name] = complexFieldConfig.value || '';
        });
        configValue.push(complexFieldObj);
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
       * Returns true if dependsOn configuration contains value in triggeredByValues.
       *
       * @param stageInstance
       * @param configuration
       * @returns {*}
       */
      verifyDependsOn: function(stageInstance, configuration) {
        var dependsOnConfigName = configuration.dependsOn,
          triggeredByValues = configuration.triggeredByValues,
          dependsOnConfiguration = _.find(stageInstance.configuration, function(config) {
          return config.name === dependsOnConfigName;
        });

        return dependsOnConfiguration && dependsOnConfiguration.value !== undefined &&
          _.contains(triggeredByValues, dependsOnConfiguration.value);
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
        var dependsOnConfigName = customConfiguration.dependsOn,
          triggeredByValues = customConfiguration.triggeredByValues,
          dependsOnConfiguration = customFieldConfigValue[dependsOnConfigName];

        return dependsOnConfiguration && _.contains(triggeredByValues, dependsOnConfiguration);
      },

      /**
       * Returns Config Model Object
       *
       * @param stageInstance
       * @param configuration
       * @returns {*}
       */
      getConfigIndex: function(stageInstance, configuration) {
        if(stageInstance && configuration) {
          var configIndex;
          _.find(stageInstance.configuration, function(config, index) {
            if(configuration.name === config.name) {
              configIndex = index;
              return true;
            }
          });

          return configIndex;
        }
      },


      /**
       * Returns filtered & sorted Group Configurations.
       *
       * @param stageInstance
       * @param configDefinitions
       * @param groupName
       * @returns {*}
       */
      isGroupVisible: function(stageInstance, configDefinitions, groupName) {
        var visible = false;

        angular.forEach(configDefinitions, function(configDefinition) {
          if(configDefinition.group === groupName &&
            (!configDefinition.dependsOn || $scope.verifyDependsOn(stageInstance, configDefinition))) {
            visible = true;
          }
        });

        return visible;
      },


      showConfigurationWarning: function(stageInstance, groupName, errorStage) {
        var config = $scope.pipelineConfig,
          commonErrors = $rootScope.common.errors,
          issuesMap,
          issues;


        if(commonErrors && commonErrors.length && commonErrors[0].pipelineIssues) {
          issuesMap = commonErrors[0];
        } else if(config && config.issues){
          issuesMap = config.issues;
        }

        if(issuesMap) {
          if(stageInstance.instanceName && issuesMap.stageIssues && issuesMap.stageIssues[stageInstance.instanceName]) {
            issues = issuesMap.stageIssues[stageInstance.instanceName];
          } else if(issuesMap.pipelineIssues) {
            issues = issuesMap.pipelineIssues;
          }
        }

        if(errorStage) {
          return issues && issues.length;
        } else {
          return _.find(issues, function(issue) {
            return issue.configGroup === groupName;
          });
        }
      },

      getCharacterValue: function(val) {
        if(val !== '\t' && val !== ';' && val !== ',' && val !== ' ') {
          return 'Other';
        }

        return val;
      }
    });


    var getFieldPaths = function(record, fieldPaths) {
      angular.forEach(record.value, function(value, key) {
        if(value.path) {
          fieldPaths.push(value.path);
        }
        if(value.type === 'MAP' || value.type === 'LIST') {
          getFieldPaths(value, fieldPaths);
        }
      });
    };

    /**
     * Update Stage Preview Data when stage selection changed.
     *
     * @param stageInstance
     */
    var updateFieldDataForStage = function(stageInstance) {
      //In case of processors and targets run the preview to get input fields & if current state of config is previewable.
      if(stageInstance.uiInfo.stageType !== pipelineConstant.SOURCE_STAGE_TYPE && $scope.pipelineConfig.previewable) {

        previewService.getInputRecordsFromPreview($scope.activeConfigInfo.name, stageInstance, 10).
          then(function (inputRecords) {
            if(_.isArray(inputRecords) && inputRecords.length) {
              var fieldPaths = [];
              getFieldPaths(inputRecords[0].value, fieldPaths);
              $scope.fieldPaths = fieldPaths;
            }
          },
          function(res) {
            $rootScope.common.errors = [res.data];
          });
      } else {

      }
    };

    var initializeGroupInformation = function(options) {
      var groupDefn = $scope.detailPaneConfigDefn.configGroupDefinition;

      if(groupDefn && groupDefn.groupNameToLabelMapList) {
        $scope.showGroups = (groupDefn.groupNameToLabelMapList.length > 0);

        $scope.configGroupTabs = angular.copy(groupDefn.groupNameToLabelMapList);

        $scope.autoFocusConfigGroup = options.configGroup;
        $scope.autoFocusConfigName = options.configName;

        if(options.configGroup) {
          angular.forEach($scope.configGroupTabs, function(groupMap) {
            if(groupMap.name === options.configGroup) {
              groupMap.active = true;
            }
          });
        }

      } else {
        $scope.showGroups = false;
        $scope.configGroupTabs = [];
      }

      $scope.errorStageConfigActive = options.errorStage;

    };

    $scope.$on('onSelectionChange', function(event, options) {
      initializeGroupInformation(options);
      if (options.type === pipelineConstant.STAGE_INSTANCE) {
        fieldsPathList = undefined;
        $scope.fieldPaths = [];
      }
    });

    if($scope.detailPaneConfigDefn) {
      initializeGroupInformation({});
    }

  }).

  controller('FieldSelectorModalInstanceController', function ($scope, $timeout, $modalInstance, previewService,
                                                               currentSelectedPaths, activeConfigInfo, detailPaneConfig) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      showLoading: true,
      noPreviewRecord: false,
      recordObject: {},
      selectedPath:_.reduce(currentSelectedPaths, function(obj, path){
        obj[path] = true;
        return obj;
      }, {}),

      save: function() {
        var selectedFieldPaths = [];
        angular.forEach($scope.selectedPath, function(value, key) {
          if(value === true) {
            selectedFieldPaths.push(key);
          }
        });

        $modalInstance.close(selectedFieldPaths);
      },

      close: function() {
        $modalInstance.dismiss('cancel');
      }
    });

    $timeout(function() {
      previewService.getInputRecordsFromPreview(activeConfigInfo.name, detailPaneConfig, 10).
        then(
          function (inputRecords) {
            $scope.showLoading = false;
            if(_.isArray(inputRecords) && inputRecords.length) {
              $scope.recordObject = inputRecords[0];
            } else {
              $scope.noPreviewRecord = true;
            }
          },
          function(res) {
            $scope.showLoading = false;
            $scope.common.errors = [res.data];
          }
        );
    }, 300);
  });