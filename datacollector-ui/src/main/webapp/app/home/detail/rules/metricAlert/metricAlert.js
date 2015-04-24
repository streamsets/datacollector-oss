/**
 * Controller for Metric Alert Rules tab.
 */

angular
  .module('dataCollectorApp.home')

  .controller('MetricAlertRulesController', function ($scope, pipelineConstant, pipelineService, $modal) {
    angular.extend($scope, {
      showLoading: false,

      /**
       * Refresh Rules
       */
      refreshRules: function() {
        updateRules($scope.activeConfigInfo.name);
      },

      /**
       * Callback function for Create New Metric Alert Rule button.
       */
      createMetricAlertRule: function() {
        if($scope.selectedType !== pipelineConstant.LINK) {

          var modalInstance = $modal.open({
            templateUrl: 'app/home/detail/rules/metricAlert/editMetricAlertRule.tpl.html',
            controller: 'CreateMetricAlertRuleModalInstanceController',
            size: 'lg',
            backdrop: 'static',
            resolve: {
              edge: function () {
                return $scope.selectedObject;
              },
              metricElementList: function() {
                return $scope.metricElementList;
              },
              metricIDList: function() {
                return $scope.metricIDList;
              },
              rulesElMetadata: function() {
                return pipelineService.getMetricRulesElMetadata();
              }
            }
          });

          modalInstance.result.then(function (metricAlertRuleDefn) {

            $scope.pipelineRules.metricsRuleDefinitions.push(metricAlertRuleDefn);
          }, function () {

          });
        }
      },

      /**
       * Callback function for Edit Data Rule button.
       */
      editMetricAlertDataRule: function(metricAlertRuleDefn, index) {
        if($scope.selectedType !== pipelineConstant.LINK) {

          var modalInstance = $modal.open({
            templateUrl: 'app/home/detail/rules/metricAlert/editMetricAlertRule.tpl.html',
            controller: 'EditMetricAlertRuleModalInstanceController',
            size: 'lg',
            backdrop: 'static',
            resolve: {
              metricAlertRuleDefn: function () {
                return angular.copy(metricAlertRuleDefn);
              },
              metricElementList: function() {
                return $scope.metricElementList;
              },
              metricIDList: function() {
                return $scope.metricIDList;
              },
              rulesElMetadata: function() {
                return pipelineService.getMetricRulesElMetadata();
              }
            }
          });

          modalInstance.result.then(function (newDataRuleDefn) {
            $scope.pipelineRules.metricsRuleDefinitions[index] = newDataRuleDefn;
          }, function () {

          });
        }
      },

      /**
       * Remove Callback function
       *
       * @param ruleList
       * @param $index
       */
      removeRule: function(ruleList, $index) {
        ruleList.splice($index, 1);
      },

      getMetricIdLabel: function(metricAlertRule) {
        var metricId = _.find($scope.metricIDList[metricAlertRule.metricType], function(metricIdObj) {
          return metricIdObj.value === metricAlertRule.metricId;
        });

        if(metricId) {
          return metricId.label;
        }
      },


      getMetricElementLabel: function(metricAlertRule) {
        var metricElement = _.find($scope.metricElementList[metricAlertRule.metricType], function(metricElementObj) {
          return metricElementObj.value === metricAlertRule.metricElement;
        });

        if(metricElement) {
          return metricElement.label;
        }
      }
    });

    $scope.metricElementList = pipelineService.getMetricElementList();


    function updateMetricIDList() {
      if($scope.pipelineConfig) {
        $scope.metricIDList = pipelineService.getMetricIDList($scope.pipelineConfig);
      }
    }

    $scope.$on('onSelectionChange', function(event, options) {
      updateMetricIDList();
    });

    updateMetricIDList();

  })

  .controller('CreateMetricAlertRuleModalInstanceController', function ($scope, $modalInstance, $translate, edge,
                                                                        $timeout, metricElementList, metricIDList,
                                                                        pipelineService, rulesElMetadata) {

    angular.extend($scope, {
      showLoading: false,
      common: {
        errors: []
      },
      metricElementList: metricElementList,
      metricIDList: metricIDList,
      metricAlertRuleDefn: {
        id: edge.info.name + (new Date()).getTime(),
        alertText: '',
        condition: '${value() > 1000}',
        metricId: null,
        metricType: 'COUNTER',
        metricElement: null,
        enabled: false,
        sendEmail: false
      },

      refreshCodemirror: false,

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
        $modalInstance.close($scope.metricAlertRuleDefn);
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });

    $scope.$broadcast('show-errors-check-validity');
  })

  .controller('EditMetricAlertRuleModalInstanceController', function ($scope, $modalInstance, $translate,
                                                                      pipelineService, $timeout, metricAlertRuleDefn,
                                                                      metricElementList, metricIDList, rulesElMetadata) {

    angular.extend($scope, {
      showLoading: false,
      common: {
        errors: []
      },
      metricElementList: metricElementList,
      metricIDList: metricIDList,
      metricAlertRuleDefn: metricAlertRuleDefn,
      refreshCodemirror: false,

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
        $modalInstance.close($scope.metricAlertRuleDefn);
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });

    $scope.$broadcast('show-errors-check-validity');
  });