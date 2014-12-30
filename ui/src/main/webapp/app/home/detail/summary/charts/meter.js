/**
 * Controller for Meter Bar Chart.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('MeterBarChartController', function($scope, $rootScope, pipelineConstant) {
    var color = $scope.recordsColor;
    
    angular.extend($scope, {
      chartData: [],

      getColor: function() {
        return function(d) {
          if(color[d.key]) {
            return color[d.key];
          } else {
            return color.Output;
          }
        };
      },

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          return '<p>' + key + ' Records </p><p>' + $scope.getDurationLabel(x) + ' throughput: ' + e.value.toFixed(2) + '</p>';
        };
      }
    });

    $scope.$on('summaryDataUpdated', function() {
      var stageInstance = $scope.detailPaneConfig,
        pipelineMetrics = $rootScope.common.pipelineMetrics,
        input = {
          key: "Input",
          values: [
            ["1m" , $scope.summaryMeters.inputRecords.m1_rate ],
            ["5m" , $scope.summaryMeters.inputRecords.m5_rate ],
            ["15m" , $scope.summaryMeters.inputRecords.m15_rate ],
            ["30m" , $scope.summaryMeters.inputRecords.m30_rate ],
            ["1h" , $scope.summaryMeters.inputRecords.h1_rate ],
            ["6h" , $scope.summaryMeters.inputRecords.h6_rate ],
            ["12h" , $scope.summaryMeters.inputRecords.h12_rate ],
            ["1d" , $scope.summaryMeters.inputRecords.h24_rate ],
            ["Mean" , $scope.summaryMeters.inputRecords.mean_rate ]
          ]
        },
        output = {
          key: "Output",
          values: [
            ["1m" , $scope.summaryMeters.outputRecords.m1_rate ],
            ["5m" , $scope.summaryMeters.outputRecords.m5_rate ],
            ["15m" , $scope.summaryMeters.outputRecords.m15_rate ],
            ["30m" , $scope.summaryMeters.outputRecords.m30_rate ],
            ["1h" , $scope.summaryMeters.outputRecords.h1_rate ],
            ["6h" , $scope.summaryMeters.outputRecords.h6_rate ],
            ["12h" , $scope.summaryMeters.outputRecords.h12_rate ],
            ["1d" , $scope.summaryMeters.outputRecords.h24_rate ],
            ["Mean" , $scope.summaryMeters.outputRecords.mean_rate ]
          ]
        },
        bad = {
          key: "Bad",
          values: [
            ["1m" , $scope.summaryMeters.errorRecords.m1_rate ],
            ["5m" , $scope.summaryMeters.errorRecords.m5_rate ],
            ["15m" , $scope.summaryMeters.errorRecords.m15_rate ],
            ["30m" , $scope.summaryMeters.errorRecords.m30_rate ],
            ["1h" , $scope.summaryMeters.errorRecords.h1_rate ],
            ["6h" , $scope.summaryMeters.errorRecords.h6_rate ],
            ["12h" , $scope.summaryMeters.errorRecords.h12_rate ],
            ["1d" , $scope.summaryMeters.errorRecords.h24_rate ],
            ["Mean" , $scope.summaryMeters.errorRecords.mean_rate ]
          ]
        };

      if($scope.stageSelected) {
        switch(stageInstance.uiInfo.stageType) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            $scope.chartData = [ output, bad];
            break;
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            $scope.chartData = [input];

            if(stageInstance.outputLanes.length < 2) {
              $scope.chartData.push(output);
            } else {
              //Lane Selector
              angular.forEach(stageInstance.outputLanes, function(outputLane, index) {
                var laneMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + ':' + outputLane + '.outputRecords.meter'];
                if(laneMeter) {
                  $scope.chartData.push({
                    key: "Output " + (index + 1),
                    values: [
                      ["1m" , laneMeter.m1_rate ],
                      ["5m" , laneMeter.m5_rate ],
                      ["15m" , laneMeter.m15_rate ],
                      ["30m" , laneMeter.m30_rate ],
                      ["1h" , laneMeter.h1_rate ],
                      ["6h" , laneMeter.h6_rate ],
                      ["12h" , laneMeter.h12_rate ],
                      ["1d" , laneMeter.h24_rate ],
                      ["Mean" , laneMeter.mean_rate ]
                    ]
                  });
                }
              });
            }

            $scope.chartData.push(bad);
            break;
          case pipelineConstant.TARGET_STAGE_TYPE:
            $scope.chartData = [ input, bad];
            break;
        }
      } else {
        $scope.chartData = [ input, output, bad];
      }

    });

  });