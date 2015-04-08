/**
 * Controller for Data Rules tab.
 */

angular
  .module('dataCollectorApp.home')

  .controller('DataRulesController', function ($scope, $rootScope, $modal, pipelineConstant,
                                               pipelineService, previewService) {
    var stageInstances = $scope.pipelineConfig.stages;

    angular.extend($scope, {
      showLoading: false,
      filteredDataRules: [],
      streamLabelMap: _.reduce(stageInstances, function(labelMap, stageInstance){
        angular.forEach(stageInstance.outputLanes, function(outputLane, index) {
          labelMap[outputLane] = stageInstance.uiInfo.label + ' Output Stream ' + (index + 1);
        });

        return labelMap;
      }, {}),

      /**
       * Callback function for Create New Data Rule button.
       */
      createDataRule: function() {
        if((!$scope.fieldPaths || $scope.fieldPaths.length === 0 ) &&
          $scope.selectedType === pipelineConstant.LINK) {
          updateFieldDataForStage($scope.selectedObject);
        }

        var modalInstance = $modal.open({
          templateUrl: 'app/home/detail/rules/dataRules/editDataRule.tpl.html',
          controller: 'CreateDataRuleModalInstanceController',
          size: 'lg',
          backdrop: 'static',
          resolve: {
            laneName: function () {
              if($scope.selectedType === pipelineConstant.LINK) {
                return $scope.selectedObject.outputLane;
              } else {
                var firstStage = stageInstances.length ? stageInstances[0] : undefined;
                return firstStage && firstStage.outputLanes && firstStage.outputLanes.length ?
                  firstStage.outputLanes[0] : undefined;
              }
            },
            rulesElMetadata: function() {
              return pipelineService.getRulesElMetadata();
            },
            fieldPaths: function() {
              return $scope.fieldPaths;
            },
            streamLabelMap: function() {
              return $scope.streamLabelMap;
            }
          }
        });

        modalInstance.result.then(function (newDataRuleDefn) {
          $scope.pipelineRules.dataRuleDefinitions.push(newDataRuleDefn);
        }, function () {

        });

      },

      /**
       * Callback function for Edit Data Rule button.
       */
      editDataRule: function(dataRuleDefn, index, $event) {

        if($event) {
          $event.stopPropagation();
        }

        if(!$scope.fieldPaths || $scope.fieldPaths.length === 0 ) {
          updateFieldDataForStage($scope.selectedObject);
        }

        var modalInstance = $modal.open({
          templateUrl: 'app/home/detail/rules/dataRules/editDataRule.tpl.html',
          controller: 'EditDataRuleModalInstanceController',
          size: 'lg',
          backdrop: 'static',
          resolve: {
            dataRuleDefn: function () {
              return angular.copy(dataRuleDefn);
            },
            rulesElMetadata: function() {
              return pipelineService.getRulesElMetadata();
            },
            fieldPaths: function() {
              return $scope.fieldPaths;
            },
            streamLabelMap: function() {
              return $scope.streamLabelMap;
            }
          }
        });

        modalInstance.result.then(function (newDataRuleDefn) {
          angular.copy(newDataRuleDefn, dataRuleDefn);
        }, function () {

        });
      },

      /**
       * Remove Callback function
       *
       * @param ruleList
       * @param rule
       */
      removeRule: function(ruleList, rule, $event) {
        if($event) {
          $event.stopPropagation();
        }

        var index;

        angular.forEach(ruleList, function(r, ind) {
          if(r.id === rule.id) {
            index = ind;
          }
        });

        if(index !== undefined) {
          ruleList.splice(index, 1);
        }
      },

      /**
       * Returns filtered data rules
       *
       * @returns {*}
       */
      getFilteredDataRules: function() {
        if($scope.selectedType === pipelineConstant.LINK) {
          return _.filter($scope.pipelineRules.dataRuleDefinitions, function(rule) {
            return rule.lane === $scope.selectedObject.outputLane;
          });
        } else {
          return $scope.pipelineRules.dataRuleDefinitions;
        }
      }
    });



    /**
     * Update Stage Preview Data when stage selection changed.
     *
     * @param edge
     */
    var updateFieldDataForStage = function(edge) {
      if(edge && $scope.pipelineConfig.previewable) {

        previewService.getEdgeInputRecordsFromPreview($scope.activeConfigInfo.name, edge, 10).
          then(function (inputRecords) {
            if(_.isArray(inputRecords) && inputRecords.length) {
              var fieldPaths = [];
              pipelineService.getFieldPaths(inputRecords[0].value, fieldPaths);
              $scope.fieldPaths = fieldPaths;
              $rootScope.$broadcast('fieldPathsUpdated', fieldPaths);
            }
          },
          function(res) {
            $rootScope.common.errors = [res.data];
          });
      } else {

      }
    };

  })

  .controller('CreateDataRuleModalInstanceController', function ($scope, $modalInstance, $translate, $timeout,
                                                                 pipelineService, laneName, rulesElMetadata, fieldPaths, streamLabelMap) {

    angular.extend($scope, {
      showLoading: false,
      common: {
        errors: []
      },
      dataRuleDefn: {
        id: 'dataRule' + (new Date()).getTime(),
        label: '',
        lane: laneName,
        condition: '',
        samplingPercentage: 5,
        samplingRecordsToRetain: 10,
        alertEnabled: true,
        alertText: '',
        thresholdType: 'COUNT',
        thresholdValue: '100',
        minVolume: 1000,
        sendEmail: false,
        meterEnabled: true,
        enabled: false
      },
      fieldPaths: fieldPaths,
      refreshCodemirror: false,
      streamLabelMap: streamLabelMap,

      getCodeMirrorOptions: function() {
        return pipelineService.getDefaultELEditorOptions();
      },

      getRulesElMetadata: function() {
        $timeout(function() {
          $scope.refreshCodemirror = true;
        });
        return rulesElMetadata;
      },

      save : function () {
        $modalInstance.close($scope.dataRuleDefn);
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });

    $scope.$broadcast('show-errors-check-validity');
  })

  .controller('EditDataRuleModalInstanceController', function ($scope, $modalInstance, $translate, pipelineService,
                                                               $timeout, dataRuleDefn, rulesElMetadata, fieldPaths, streamLabelMap) {

    angular.extend($scope, {
      showLoading: false,
      common: {
        errors: []
      },
      dataRuleDefn: dataRuleDefn,
      fieldPaths: fieldPaths,
      refreshCodemirror: false,
      streamLabelMap: streamLabelMap,

      getCodeMirrorOptions: function() {
        return pipelineService.getDefaultELEditorOptions();
      },

      getRulesElMetadata: function() {
        $timeout(function() {
          $scope.refreshCodemirror = true;
        });
        return rulesElMetadata;
      },

      save : function () {
        $modalInstance.close($scope.dataRuleDefn);
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });

    $scope.$broadcast('show-errors-check-validity');
  });