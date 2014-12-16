/**
 * Controller for Meter Bar Chart.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('MeterBarChartController', function($scope, pipelineConstant) {
    var color = {
      'Input' :'#1f77b4',
      'Output': '#5cb85c',
      'Bad':'#FF3333'
    };

    angular.extend($scope, {
      chartData: [],

      getColor: function() {
        return function(d) {
          return color[d.key];
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
            $scope.chartData = [ input, output, bad];
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