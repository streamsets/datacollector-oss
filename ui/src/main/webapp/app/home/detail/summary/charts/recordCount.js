/**
 * Controller for Record Processed Bar Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('RecordCountBarChartController', function($scope, $rootScope, pipelineConstant) {
    var color = $scope.recordsColor;

    angular.extend($scope, {
      barChartData: [],

      getColor: function() {
        return function(d) {
          if(color[d[0]]) {
            return color[d[0]];
          } else {
            return color.Output;
          }
        };
      },

      getYAxisLabel: function() {
        return function() {
          return '';
        };
      }

    });

    var refreshData = function() {
      var stageInstance = $scope.detailPaneConfig,
        pipelineMetrics = $rootScope.common.pipelineMetrics,
        valueList = [],
        inputRecordsMeter = $scope.summaryMeters.inputRecords,
        outputRecordsMeter = $scope.summaryMeters.outputRecords,
        errorRecordsMeter = $scope.summaryMeters.errorRecords;

      if(!inputRecordsMeter || !outputRecordsMeter || !errorRecordsMeter) {
        return;
      }

      if($scope.stageSelected) {
        switch(stageInstance.uiInfo.stageType) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            valueList.push(["Output" , outputRecordsMeter.count ]);
            valueList.push(["Bad" , errorRecordsMeter.count ]);
            break;
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            valueList.push(["Input" , inputRecordsMeter.count ]);

            if(stageInstance.outputLanes.length < 2) {
              valueList.push(["Output" , outputRecordsMeter.count ]);
            } else {
              //Lane Selector
              angular.forEach(stageInstance.outputLanes, function(outputLane, index) {
                var laneMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + ':' + outputLane + '.outputRecords.meter'];
                if(laneMeter) {
                  valueList.push(["Output " + (index + 1), laneMeter.count ]);
                }
              });
            }

            valueList.push(["Bad" , errorRecordsMeter.count ]);
            break;
          case pipelineConstant.TARGET_STAGE_TYPE:
            valueList.push(["Input" , inputRecordsMeter.count ]);
            valueList.push(["Bad" , errorRecordsMeter.count ]);
            break;
        }
      } else {
        valueList.push(["Input" , inputRecordsMeter.count ]);
        valueList.push(["Output" , outputRecordsMeter.count ]);
        valueList.push(["Bad" , errorRecordsMeter.count ]);
      }

      $scope.barChartData = [
        {
          key: "Processed Records",
          values: valueList
        }
      ];
    };


    $scope.$on('summaryDataUpdated', function() {
      refreshData();
    });

    if($scope.summaryMeters) {
      refreshData();
    }

  });