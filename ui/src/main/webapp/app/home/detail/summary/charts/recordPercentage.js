/**
 * Controller for Record Percentage Pie Chart.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('RecordPercentagePieChartController', function($scope) {
    var colorArray = ['#5cb85c', '#FF3333'];

    angular.extend($scope, {
      pieChartData: [],

      getLabel: function(){
        return function(d) {
          switch(d.key) {
            case 'goodRecords':
              return 'Good Records';
            case 'badRecords':
              return 'Bad Records';
          }
        };
      },

      getValue: function() {
        return function(d){
          return d.value;
        };
      },

      getColor: function() {
        return function(d, i) {
          return colorArray[i];
        };
      },

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          return '<p>' + key + '</p><p>' + x +  '</p>';
        };
      }
    });

    $scope.$on('summaryDataUpdated', function() {
      $scope.pieChartData = [
        {
          key: "goodRecords",
          value: $scope.summaryMeters.outputRecords.count || 1
        },
        {
          key: "badRecords",
          value: $scope.summaryMeters.errorRecords.count
        }
      ];
    });

  });