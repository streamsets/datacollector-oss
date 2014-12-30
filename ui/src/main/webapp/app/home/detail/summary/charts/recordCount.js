/**
 * Controller for Record Processed Bar Chart.
 */

angular
  .module('pipelineAgentApp.home')
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

    $scope.$on('summaryDataUpdated', function() {
      var stageInstance = $scope.detailPaneConfig,
        pipelineMetrics = $rootScope.common.pipelineMetrics,
        valueList = [];

      if($scope.stageSelected) {
        switch(stageInstance.uiInfo.stageType) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            valueList.push(["Output" , $scope.summaryMeters.outputRecords.count ]);
            valueList.push(["Bad" , $scope.summaryMeters.errorRecords.count ]);
            break;
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            valueList.push(["Input" , $scope.summaryMeters.inputRecords.count ]);

            if(stageInstance.outputLanes.length < 2) {
              valueList.push(["Output" , $scope.summaryMeters.outputRecords.count ]);
            } else {
              //Lane Selector
              angular.forEach(stageInstance.outputLanes, function(outputLane, index) {
                var laneMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + ':' + outputLane + '.outputRecords.meter'];
                if(laneMeter) {
                  valueList.push(["Output " + (index + 1), laneMeter.count ]);
                }
              });
            }

            valueList.push(["Bad" , $scope.summaryMeters.errorRecords.count ]);
            break;
          case pipelineConstant.TARGET_STAGE_TYPE:
            valueList.push(["Input" , $scope.summaryMeters.inputRecords.count ]);
            valueList.push(["Bad" , $scope.summaryMeters.errorRecords.count ]);
            break;
        }
      } else {
        valueList.push(["Input" , $scope.summaryMeters.inputRecords.count ]);
        valueList.push(["Output" , $scope.summaryMeters.outputRecords.count ]);
        valueList.push(["Bad" , $scope.summaryMeters.errorRecords.count ]);
      }

      $scope.barChartData = [
        {
          key: "Processed Records",
          values: valueList
        }
      ];
    });

  });