/**
 * Controller for Configuration.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('ConfigurationController', function ($scope, $rootScope, $q, $modal, _,
                                                   api, previewService, pipelineConstant) {
    var fieldsPathList;

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
          issues,
          issue;

        if(config && config.issues) {
          if(configObject.instanceName && config.issues.stageIssues &&
            config.issues.stageIssues && config.issues.stageIssues[configObject.instanceName]) {
            issues = config.issues.stageIssues[configObject.instanceName];
          } else if(config.issues.pipelineIssues){
            issues = config.issues.pipelineIssues;
          }
        }

        issue = _.find(issues, function(issue) {
          return (issue.level === 'STAGE_CONFIG' && issue.configName === configDefinition.name);
        });

        return issue ? issue.message : '';
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
       * Field Selector autocomplete callback functiton.
       *
       * @param query
       * @returns {*}
       */
      loadFields: function(query) {
        var deferred = $q.defer();

        if(!fieldsPathList) {
          console.log('loading first time');
          previewService.getInputRecordsFromPreview($scope.activeConfigInfo.name, $scope.detailPaneConfig, 10).
            then(
              function (inputRecords) {
                var fieldPaths = [];

                if(_.isArray(inputRecords) && inputRecords.length) {
                  getFieldPaths(inputRecords[0].value, fieldPaths);
                  fieldsPathList = fieldPaths;
                }

                deferred.resolve(fieldPaths);
              },
              function(res) {
                $rootScope.common.errors = [res.data];
              }
            );
        } else {
          console.log('Using already loaded one');
          var filteredList =  _.filter(fieldsPathList, function(fieldPath) {
            return fieldPath.text.indexOf(query) !== -1;
          });

          deferred.resolve(filteredList);
        }

        return deferred.promise;
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


      addLane: function(stageInstance, configValue) {
        var outputLaneName = stageInstance.instanceName + 'OutputLane' + (new Date()).getTime();
        stageInstance.outputLanes.push(outputLaneName);
        configValue.push({
          outputLane: outputLaneName,
          predicate: ''
        });
      },

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

      addToMap: function(stageInstance, configValue) {
        configValue.push({
          key: '',
          value: ''
        });
      },

      removeFromMap: function(stageInstance, configValue, mapObject, $index) {
        configValue.splice($index, 1);
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
      }
    };

    $scope.$on('onStageSelection', function(event, stageInstance) {
      if (stageInstance) {
        fieldsPathList = undefined;

        $scope.fieldPaths = [];
        updateFieldDataForStage(stageInstance);
      }
    });


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
        console.log($scope.selectedPath);

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