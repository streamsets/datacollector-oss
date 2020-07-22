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
 * Controller for Metric Alert Rules tab.
 */

angular
  .module('dataCollectorApp.home')

  .controller('MetricAlertRulesController', function (
      $scope, pipelineConstant, pipelineService, $modal, tracking,
      trackingEvent
    ) {
    angular.extend($scope, {
      showLoading: false,

      /**
       * Refresh Rules
       */
      refreshRules: function() {
        updateRules($scope.activeConfigInfo.pipelineId);
      },

      /**
       * Callback function for Create New Metric Alert Rule button.
       */
      createMetricAlertRule: function() {
        if($scope.selectedType !== pipelineConstant.LINK) {

          $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Add Metric Alert Rule', 1);
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
            $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Save Metric Alert Rule', 1);
            $scope.pipelineRules.metricsRuleDefinitions.push(metricAlertRuleDefn);
            tracking.mixpanel.track(trackingEvent.METRIC_RULE_CREATED, {'Pipeline ID': $scope.pipelineConfig.pipelineId});
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
        var codeMirrorOptions = {
          dictionary: rulesElMetadata,
          extraKeys: {
            'Tab': false,
            'Ctrl-Space': 'autocomplete'
          }
        };

        $timeout(function() {
          $scope.refreshCodemirror = true;
        });

        return angular.extend({}, pipelineService.getDefaultELEditorOptions(), codeMirrorOptions);
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
        var codeMirrorOptions = {
          dictionary: rulesElMetadata,
          extraKeys: {
            'Tab': false,
            'Ctrl-Space': 'autocomplete'
          }
        };

        $timeout(function() {
          $scope.refreshCodemirror = true;
        });

        return angular.extend({}, pipelineService.getDefaultELEditorOptions(), codeMirrorOptions);
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
