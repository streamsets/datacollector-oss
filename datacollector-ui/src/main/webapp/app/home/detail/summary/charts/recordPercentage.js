/**
 * Controller for Record Percentage Pie Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('RecordPercentagePieChartController', function($scope) {
    var colorArray = ['#5cb85c', '#FF3333'];

    angular.extend($scope, {
      allDataZero: false,
      chartOptions: {
        chart: {
          type: 'pieChart',
          height: 250,
          x: function(d) {
            switch(d.key) {
              case 'goodRecords':
                return 'Good Records';
              case 'errorRecords':
                return 'Error Records';
            }
          },
          y: function(d){
            return d.value;
          },
          showLabels: true,
          color: function(d, i) {
            return colorArray[i];
          },
          legendColor: function(d, i) {
            return colorArray[i];
          },
          showLegend: true,
          labelType: "percent",
          donut: true,
          labelsOutside: true,
          transitionDuration: 500,
          labelThreshold: 0.01,
          legend: {
            margin: {
              left:10,
              top:10,
              bottom:10,
              right:10
            }
          }
        }
      },
      pieChartData: [
        {
          key: "goodRecords",
          value: 0
        },
        {
          key: "errorRecords",
          value: 0
        }
      ]
    });

    var refreshChartData = function() {
      $scope.allDataZero = true;
      if($scope.summaryMeters.outputRecords && $scope.summaryMeters.errorRecords) {
        angular.forEach($scope.pieChartData, function(chartData) {
          if(chartData.key === 'goodRecords') {
            chartData.value = $scope.summaryMeters.outputRecords.count;
          } else if(chartData.key === 'errorRecords') {
            chartData.value = $scope.summaryMeters.errorRecords.count;
          }

          if(chartData.value > 0) {
            $scope.allDataZero = false;
          }
        });
      }
    };


    $scope.$on('summaryDataUpdated', function() {
      refreshChartData();
    });

    refreshChartData();

  });