/**
 * Controller for Batch Timer Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('AllStageMemoryConsumedChartController', function($rootScope, $scope) {

    angular.extend($scope, {
      chartData: [],

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          var mbValue = e.value / 1000000;
          return '<p>Heap Memory Consumed</p><p>' + $scope.getDurationLabel(x) + ' : ' + mbValue.toFixed(1) + ' MB' + '</p>';
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
      var pipelineMetrics = $rootScope.common.pipelineMetrics,
        stages = $scope.pipelineConfig.stages,
        values = [];

      if(!pipelineMetrics.counters) {
        return;
      }

      angular.forEach(stages, function(stage) {
        var stageCounter = pipelineMetrics.counters['stage.' + stage.instanceName + '.memoryConsumed.counter'];

        if(stageCounter) {
          values.push([stage.uiInfo.label,
            stageCounter.count
          ]);
        }
      });

      $scope.chartData = [
        {
          key: "Heap Memory Consumed",
          values: values
        }
      ];
    });
  });