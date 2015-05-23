/**
 * Controller for Batch Timer Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('AllStageMemoryConsumedChartController', function($rootScope, $scope) {


    angular.extend($scope, {
      chartData: [],

      getLabel: function(){
        return function(d) {
          return d.key;
        };
      },

      getValue: function() {
        return function(d){
          if(d.value > 0) {
            return d.value.toFixed(2);
          } else {
            return 0;
          }
        };
      },

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          var mbValue = y.value /1;
          return '<p>' + key + '</p><p>' +  mbValue.toFixed(2) + ' MB </p>';
        };
      }
    });


    var stages = $scope.pipelineConfig.stages;

    angular.forEach(stages, function(stage) {
      $scope.chartData.push({
        instanceName: stage.instanceName,
        key: stage.uiInfo.label,
        value: 0
      });
    });

    $scope.$on('summaryDataUpdated', function() {
      var pipelineMetrics = $rootScope.common.pipelineMetrics,
        values = [],
        total = 0;

      if(!pipelineMetrics.counters) {
        return;
      }

      angular.forEach($scope.chartData, function(data) {
        var stageCounter = pipelineMetrics.counters['stage.' + data.instanceName + '.memoryConsumed.counter'];
        if(stageCounter) {
          data.value =  (stageCounter.count / 1000000);
        }
        values.push(data);
        total += stageCounter.count;
      });

      $scope.chartData = values;
      $scope.totalValue = (total/1000000).toFixed(2);
    });
  });