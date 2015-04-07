/**
 * Controller for Record Processed Bar Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('MemoryConsumedLineChartController', function($scope, $rootScope, pipelineConstant) {
    var color = $scope.recordsColor;

    angular.extend($scope, {
      lineChartData: [],

      getColor: function() {
        return function(d) {
          if(color[d[0]]) {
            return color[d[0]];
          } else {
            return color.Output;
          }
        };
      },

      dateFormat: function() {
        return function(d){
          return d3.time.format('%H:%M:%S')(new Date(d));
        };
      },

      sizeFormat: function(){
        return function(d){
          var mbValue = d / 1000000;
          return mbValue.toFixed(1) + ' MB';
        };
      }

    });

    $scope.$on('summaryDataUpdated', function() {
      var currentSelection = $scope.detailPaneConfig,
        memoryConsumed = $rootScope.common.counters.memoryConsumed || {},
        pipelineConfig = $scope.pipelineConfig,
        values;


      if($scope.stageSelected) {
        values =  memoryConsumed[currentSelection.instanceName];
      } else {
        values = memoryConsumed['pipeline.' + pipelineConfig.info.name];
      }

      if(!values) {
        return;
      }

      $scope.lineChartData = [
        {
          key: "Total",
          values: values
        }
      ];
      $scope.xAxisTickFormat = $scope.dateFormat();
      $scope.yAxisTickFormat = $scope.sizeFormat();
    });

  });
