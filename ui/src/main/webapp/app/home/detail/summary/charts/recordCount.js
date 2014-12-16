/**
 * Controller for Record Processed Bar Chart.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('RecordCountBarChartController', function($scope, pipelineConstant) {
    var color = {
      'Input' :'#1f77b4',
      'Output': '#5cb85c',
      'Bad':'#FF3333'
    };

    angular.extend($scope, {
      barChartData: [],

      getColor: function() {
        return function(d) {
          return color[d[0]];
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
        valueList = [];

      if($scope.stageSelected) {
        switch(stageInstance.uiInfo.stageType) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            valueList.push(["Output" , $scope.summaryMeters.outputRecords.count ]);
            valueList.push(["Bad" , $scope.summaryMeters.errorRecords.count ]);
            break;
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            valueList.push(["Input" , $scope.summaryMeters.inputRecords.count ]);
            valueList.push(["Output" , $scope.summaryMeters.outputRecords.count ]);
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