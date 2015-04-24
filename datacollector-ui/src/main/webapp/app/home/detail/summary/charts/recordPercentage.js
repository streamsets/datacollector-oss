/**
 * Controller for Record Percentage Pie Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('RecordPercentagePieChartController', function($scope) {
    var colorArray = ['#5cb85c', '#FF3333'];

    angular.extend($scope, {
      pieChartData: [],

      getLabel: function(){
        return function(d) {
          switch(d.key) {
            case 'goodRecords':
              return 'Good Records';
            case 'errorRecords':
              return 'Error Records';
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
          return '<p>' + key + '</p><p>' + y.value +  '</p>';
        };
      }
    });

    $scope.$on('summaryDataUpdated', function() {
      if($scope.summaryMeters.outputRecords && $scope.summaryMeters.errorRecords) {
        $scope.pieChartData = [
          {
            key: "goodRecords",
            value: $scope.summaryMeters.outputRecords.count
          },
          {
            key: "errorRecords",
            value: $scope.summaryMeters.errorRecords.count
          }
        ];
      }

    });

  });