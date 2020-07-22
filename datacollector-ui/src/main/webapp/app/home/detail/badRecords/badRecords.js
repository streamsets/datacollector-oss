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
 * Controller for Bad Records Tab.
 */

angular
  .module('dataCollectorApp.home')

  .controller('BadRecordsController', function ($scope, $rootScope, _, api, pipelineConstant, $filter, $timeout, $modal) {

    var formatValue = function(d){
      return $filter('abbreviateNumber')(d);
    };

    angular.extend($scope, {
      percentilesChartOptions: {
        chart: {
          type: 'multiBarHorizontalChart',
          stacked: true,
          height: 250,
          showLabels: true,
          duration: 0,
          x: function(d) {
            return d[0];
          },
          y: function(d) {
            return d[1];
          },
          showLegend: true,
          staggerLabels: true,
          showValues: false,
          yAxis: {
            tickValues: 0
          },
          margin: {
            left: 55,
            top: 20,
            bottom: 20,
            right: 20
          },
          reduceXTicks: false,
          showControls: false
        }
      },

      barChartOptions: {
        chart: {
          type: 'discreteBarChart',
          showControls: false,
          height: 250,
          showLabels: true,
          duration: 0,
          x: function(d) {
            return d[0];
          },
          y: function(d) {
            return d[1];
          },
          staggerLabels: true,
          showValues: true,
          yAxis: {
            tickValues: 0
          },
          margin: {
            left: 40,
            top: 20,
            bottom: 40,
            right: 20
          },
          reduceXTicks: false
        }
      },


      showBadRecordsLoading: false,
      showErrorMessagesLoading: false,
      badRecordsChartData: [{
        key: "Bad Records",
        values: []
      }],
      errorMessagesChartData: [{
        key: "Error Messages",
        values: []
      }],
      errorMessagesCount: 0,
      errorMessages: [],
      errorRecordsCount: 0,
      expandAllErrorData: false,
      stageBadRecords:[],

      errorRecordsPercentilesData: [
        {
          key: 'Error Records',
          values: [],
          color: '#FF3333'
        }
      ],

      errorsPercentilesData: [
        {
          key: 'Stage Errors',
          values: [],
          color: '#d62728'
        }
      ],

      onExpandAllErrorData: function() {
        $scope.expandAllErrorData = true;
      },

      onCollapseAllErrorData: function() {
        $scope.expandAllErrorData = false;
      },

      refreshBadRecordsData: function() {
        var currentSelection = $scope.detailPaneConfig;
        updateBadRecordsData(currentSelection);
      },

      refreshErrorMessagesData: function() {
        var currentSelection = $scope.detailPaneConfig;
        updateErrorMessagesData(currentSelection);

      },

      /**
       * Display stack trace in modal dialog.
       *
       * @param errorMessage
       */
      showStackTrace: function (errorMessage) {
        $modal.open({
          templateUrl: 'errorModalContent.html',
          controller: 'ErrorModalInstanceController',
          size: 'lg',
          backdrop: true,
          resolve: {
            errorObj: function () {
              return {
                RemoteException: {
                  antennaDoctorMessages: errorMessage.antennaDoctorMessages,
                  localizedMessage: errorMessage.localized,
                  stackTrace: errorMessage.errorStackTrace
                }
              };
            }
          }
        });
      },

      getYAxisLabel: function() {
        return function() {
          return '';
        };
      },

      /**
       * Callback function when tab is selected.
       */
      onTabSelect: function() {
        $scope.errorDataLoaded = false;
        $timeout(function() {
          $scope.errorDataLoaded = true;
        });
      }
    });

    var updateBadRecordsData = function(currentSelection) {
      $scope.showBadRecordsLoading = true;
      api.pipelineAgent.getErrorRecords(
        $scope.pipelineConfig.info.pipelineId,
        0,
        currentSelection.instanceName,
        $scope.edgeHttpUrl
      ).then(function(response) {
        var res = response.data;
        $scope.showBadRecordsLoading = false;
        if (res && res.length) {
          $scope.stageBadRecords = res.reverse();
        } else {
          $scope.showBadRecordsLoading = [];
        }
      }).catch(function(res) {
        $scope.showBadRecordsLoading = false;
        $rootScope.common.errors = [res.data];
      });
    };

    var updateErrorMessagesData = function(currentSelection) {
      $scope.showErrorMessagesLoading = true;
      api.pipelineAgent.getErrorMessages(
        $scope.pipelineConfig.info.pipelineId,
        0,
        currentSelection.instanceName,
        $scope.edgeHttpUrl
      ).then(function(response) {
        var res = response.data;
        $scope.showErrorMessagesLoading = false;
        if (res && res.length) {
          $scope.errorMessages = res.reverse();
        } else {
          $scope.errorMessages = [];
        }
      }).catch(function(res) {
        $scope.showErrorMessagesLoading = false;
        $rootScope.common.errors = [res.data];
      });
    };

    var updateErrorsTabData = function(options) {
      var type = options.type;
      var pipelineMetrics = $scope.detailPaneMetrics;
      var currentSelection = $scope.detailPaneConfig;
      if (pipelineMetrics && pipelineMetrics.meters) {
        if (type === pipelineConstant.STAGE_INSTANCE) {  //Stage Instance
          //Bad Records
          var errorRecordsCount = $scope.errorRecordsCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.errorRecords.meter'];
          $scope.stageBadRecords = [];
          if (errorRecordsCount && parseInt(errorRecordsCount.count) > 0) {
            updateBadRecordsData(currentSelection);
          }

          //Error Messages
          var errorMessagesCount = $scope.errorMessagesCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.stageErrors.meter'];
          $scope.errorMessages = [];
          if (errorMessagesCount && parseInt(errorMessagesCount.count) > 0) {
            updateErrorMessagesData(currentSelection);
          }

          $scope.errorRecordsHistogram = pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.errorRecords.histogramM5'];
          $scope.errorsHistogram = pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.stageErrors.histogramM5'];

        } else if (type === pipelineConstant.PIPELINE){  //Pipeline
          $scope.errorRecordsCount = pipelineMetrics.meters['pipeline.batchErrorRecords.meter'];
          $scope.stageBadRecords = [];

          $scope.errorMessagesCount = pipelineMetrics.meters['pipeline.batchErrorMessages.meter'];
          $scope.errorMessages = [];
        }
      }
    };

    if ($scope.selectedType) {
      updateErrorsTabData({
        type: $scope.selectedType
      });
    }

    $scope.$on('onSelectionChange', function(event, options) {
      updateErrorsTabData(options);
    });


    $scope.$watch('detailPaneMetrics', function() {
      if (!$scope.pipelineConfig) {
        return;
      }

      var pipelineMetrics = $scope.detailPaneMetrics;
      var currentSelection = $scope.detailPaneConfig;
      var stages = $scope.stageInstances;
      var badRecordsArr = [];
      var errorMessagesArr = [];
      var errorRecordsHistogram;
      var errorsHistogram;

      if (pipelineMetrics && pipelineMetrics.meters) {
        if (currentSelection.instanceName) {
          errorRecordsHistogram = $scope.errorRecordsHistogram = pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.errorRecords.histogramM5'];
          errorsHistogram = $scope.errorsHistogram = pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.stageErrors.histogramM5'];

          $scope.errorRecordsCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.errorRecords.meter'];
          $scope.errorMessagesCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.stageErrors.meter'];
        } else {

          angular.forEach(stages, function(stage) {
            var errorRecordsMeter = pipelineMetrics.meters['stage.' + stage.instanceName + '.errorRecords.meter'],
              stageErrorsMeter = pipelineMetrics.meters['stage.' + stage.instanceName + '.stageErrors.meter'];

            if (errorRecordsMeter && stageErrorsMeter) {
              badRecordsArr.push([stage.uiInfo.label,
                errorRecordsMeter.count
              ]);

              errorMessagesArr.push([stage.uiInfo.label,
                stageErrorsMeter.count
              ]);
            }
          });

          $scope.badRecordsChartData[0].values = badRecordsArr;
          $scope.errorMessagesChartData[0].values = errorMessagesArr;

          errorRecordsHistogram = pipelineMetrics.histograms['pipeline.errorRecordsPerBatch.histogramM5'];
          errorsHistogram = pipelineMetrics.histograms['pipeline.errorsPerBatch.histogramM5'];
        }

        if (errorRecordsHistogram) {
          $scope.errorRecordsPercentilesData[0].values = [
                ["Mean" , errorRecordsHistogram.mean ],
                ["Std Dev" , errorRecordsHistogram.stddev ],
                ["99.9%" , errorRecordsHistogram.p999 ],
                ["99%" , errorRecordsHistogram.p99 ],
                ["98%" , errorRecordsHistogram.p98 ],
                ["95%" , errorRecordsHistogram.p95 ],
                ["75%" , errorRecordsHistogram.p75 ],
                ["50%" , errorRecordsHistogram.p50 ]
              ];
        }


        if (errorsHistogram) {
          $scope.errorsPercentilesData[0].values = [
                ["Mean" , errorsHistogram.mean ],
                ["Std Dev" , errorsHistogram.stddev ],
                ["99.9%" , errorsHistogram.p999 ],
                ["99%" , errorsHistogram.p99 ],
                ["98%" , errorsHistogram.p98 ],
                ["95%" , errorsHistogram.p95 ],
                ["75%" , errorsHistogram.p75 ],
                ["50%" , errorsHistogram.p50 ]
              ];
        }

      }
    });

    //Workaround for refreshing the graph after data is loaded
    $timeout(function() {
      $scope.errorDataLoaded = true;
    });

  });
