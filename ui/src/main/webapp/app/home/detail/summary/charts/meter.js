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
      }
    });

    $scope.$on('summaryDataUpdated', function() {
      var stageInstance = $scope.detailPaneConfig,
        input = {
          key: "Input",
          values: [
            ["M1" , $scope.summaryMeters.inputRecords.m1_rate ],
            ["M5" , $scope.summaryMeters.inputRecords.m5_rate ],
            ["M15" , $scope.summaryMeters.inputRecords.m15_rate ],
            ["M30" , $scope.summaryMeters.inputRecords.m30_rate ],
            ["H1" , $scope.summaryMeters.inputRecords.h1_rate ],
            ["H6" , $scope.summaryMeters.inputRecords.h6_rate ],
            ["H12" , $scope.summaryMeters.inputRecords.h12_rate ],
            ["H24" , $scope.summaryMeters.inputRecords.h24_rate ],
            ["Mean" , $scope.summaryMeters.inputRecords.mean_rate ]
          ]
        },
        output = {
          key: "Output",
          values: [
            ["M1" , $scope.summaryMeters.outputRecords.m1_rate ],
            ["M5" , $scope.summaryMeters.outputRecords.m5_rate ],
            ["M15" , $scope.summaryMeters.outputRecords.m15_rate ],
            ["M30" , $scope.summaryMeters.outputRecords.m30_rate ],
            ["H1" , $scope.summaryMeters.outputRecords.h1_rate ],
            ["H6" , $scope.summaryMeters.outputRecords.h6_rate ],
            ["H12" , $scope.summaryMeters.outputRecords.h12_rate ],
            ["H24" , $scope.summaryMeters.outputRecords.h24_rate ],
            ["Mean" , $scope.summaryMeters.outputRecords.mean_rate ]
          ]
        },
        bad = {
          key: "Bad",
          values: [
            ["M1" , $scope.summaryMeters.errorRecords.m1_rate ],
            ["M5" , $scope.summaryMeters.errorRecords.m5_rate ],
            ["M15" , $scope.summaryMeters.errorRecords.m15_rate ],
            ["M30" , $scope.summaryMeters.errorRecords.m30_rate ],
            ["H1" , $scope.summaryMeters.errorRecords.h1_rate ],
            ["H6" , $scope.summaryMeters.errorRecords.h6_rate ],
            ["H12" , $scope.summaryMeters.errorRecords.h12_rate ],
            ["H24" , $scope.summaryMeters.errorRecords.h24_rate ],
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