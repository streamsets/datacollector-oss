/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * JVMMetrics module for displaying JVM Metrics page content.
 */

angular
  .module('commonUI.jvmMetrics')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/jvmMetrics',
      {
        templateUrl: 'common/administration/jvmMetrics/jvmMetrics.tpl.html',
        controller: 'JVMMetricsController',
        resolve: {
          myVar: function(authService) {
            return authService.init();
          }
        },
        data: {
          authorizedRoles: ['admin']
        }
      }
    );
  }])
  .controller('JVMMetricsController', function (
    $scope, $rootScope, $timeout, api, configuration, Analytics, visibilityBroadcaster, $modal
  ) {
    var jvmMetricsTimer,
      destroyed = false,
      dateFormat = function(d) {
        return d3.time.format('%H:%M:%S')(new Date(d));
      },
      sizeFormat = function(d) {
        var mbValue = d / 1000000;
        return mbValue.toFixed(1) + ' MB';
      },
      cpuPercentageFormat = function(d){
        var mbValue = d * 1000/ 10.0;
        return mbValue.toFixed(1) + ' %';
      },
      formatValue = function(d, chart) {
        if(chart.yAxisTickFormat) {
          return chart.yAxisTickFormat(d);
        } else {
          return d;
        }
      },
      defaultChartOptions = {
        chart: {
          type: 'lineChart', //'multiBarChart', //'lineChart'
          height: 250,
          showLabels: true,
          showControls: false,
          duration: 0,
          x:function(d){
            return (new Date(d[0])).getTime();
          },
          y: function(d) {
            return d[1];
          },
          showLegend: true,
          xAxis: {
            tickFormat: dateFormat
          },
          yAxis: {
            //tickFormat: sizeFormat
          },
          margin: {
            left: 60,
            top: 20,
            bottom: 30,
            right: 30
          },
          useInteractiveGuideline: true
        }
      };

    configuration.init().then(function() {
      if(configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/jvmMetrics');
      }
    });

    angular.extend($scope, {
      metrics: {},
      jmxList: [],

      getChartOptions: function(metricChartObject) {
        var chartOptions = angular.copy(defaultChartOptions);
        if(metricChartObject.yAxisTickFormat) {
          chartOptions.chart.yAxis.tickFormat = metricChartObject.yAxisTickFormat;
        }
        return chartOptions;
      },

      dateFormat: function() {
        return function(d){
          return d3.time.format('%H:%M:%S')(new Date(d));
        };
      },

      sizeFormat: function(){
        return function(d){
          var mbValue = d / 1000000;
          return mbValue.toFixed(0) + ' MB';
        };
      },

      cpuPercentageFormat: function() {
        return function(d){
          var mbValue = d * 1000/ 10.0;
          return mbValue.toFixed(1) + ' %';
        };
      },

      formatValue: function(d, chart) {
        if(chart.yAxisTickFormat) {
          return chart.yAxisTickFormat(d);
        } else {
          return d;
        }
      },

      removeChart: function(chart) {
        var index = _.indexOf($rootScope.$storage.jvmMetricsChartList, chart.name);

        if(index !== -1) {
          $rootScope.$storage.jvmMetricsChartList.splice(index, 1);
        }
      },

      filterChart: function(chart) {
        return _.contains($rootScope.$storage.jvmMetricsChartList, chart.name);
      },

      /**
       * Launch Settings Modal Dialog
       */
      launchSettings: function() {
        var modalInstance = $modal.open({
          templateUrl: 'common/administration/jvmMetrics/settings/settingsModal.tpl.html',
          controller: 'JVMMetricsSettingsModalInstanceController',
          backdrop: 'static',
          size: 'lg',
          resolve: {
            availableCharts: function () {
              return $scope.chartList;
            },
            selectedCharts: function() {
              return _.filter($scope.chartList, function(chart) {
                return _.contains($rootScope.$storage.jvmMetricsChartList, chart.name);
              });
            }
          }
        });

        modalInstance.result.then(function (selectedCharts) {
          $rootScope.$storage.jvmMetricsChartList = _.pluck(selectedCharts, 'name');
        }, function () {

        });
      },


      /**
       * Launch Thread Dump Modal Dialog
       */
      launchThreadDump: function() {
        $modal.open({
          templateUrl: 'common/administration/jvmMetrics/threadDump/threadDumpModal.tpl.html',
          controller: 'ThreadDumpModalInstanceController',
          backdrop: 'static',
          size: 'lg'
        });
      }

    });

    $scope.chartList = [
      {
        name: 'cpuUsage',
        label: 'CPU Usage',
        xAxisTickFormat: $scope.dateFormat(),
        yAxisTickFormat: cpuPercentageFormat,
        values: [
          {
            name: 'java.lang:type=OperatingSystem',
            property: 'ProcessCpuLoad',
            key: 'CPU Usage',
            values: [],
            area: true
          }
        ]
      },
      {
        name: 'threads',
        label: 'Threads',
        xAxisTickFormat: $scope.dateFormat(),
        values: [
          {
            name: 'java.lang:type=Threading',
            property: 'ThreadCount',
            key: 'Live Threads',
            values: [],
            area: true
          }
        ],
        displayProperties: [
          {
            name: 'java.lang:type=Threading',
            property: 'PeakThreadCount',
            key: 'Peak',
            value:''
          },
          {
            name: 'java.lang:type=Threading',
            property: 'TotalStartedThreadCount',
            key: 'Total',
            value: ''
          }
        ]
      },
      {
        name: 'classes',
        label: 'Classes',
        xAxisTickFormat: $scope.dateFormat(),
        values: [
          {
            name: 'java.lang:type=ClassLoading',
            property: 'LoadedClassCount',
            key: 'Loaded',
            values: [],
            area: true
          },
          {
            name: 'java.lang:type=ClassLoading',
            property: 'UnloadedClassCount',
            key: 'Unloaded',
            values: []
          },
          {
            name: 'java.lang:type=ClassLoading',
            property: 'TotalLoadedClassCount',
            key: 'Total',
            values: []
          }
        ]
      },
      {
        name: 'heapMemoryUsage',
        label: 'Heap Memory Usage',
        xAxisTickFormat: $scope.dateFormat(),
        yAxisTickFormat: sizeFormat,
        values: [
          {
            name: 'metrics:name=jvm.memory.heap.max',
            property: 'Value',
            key: 'Max',
            values: []
          },
          {
            name: 'metrics:name=jvm.memory.heap.committed',
            property: 'Value',
            key: 'Committed',
            values: []
          },
          {
            name: 'metrics:name=jvm.memory.heap.used',
            property: 'Value',
            key: 'Used',
            values: [],
            area: true
          }
        ]
      },
      {
        name: 'nonHeapMemoryUsage',
        label: 'Non-Heap Memory Usage',
        xAxisTickFormat: $scope.dateFormat(),
        yAxisTickFormat: sizeFormat,
        values: [
          {
            name: 'metrics:name=jvm.memory.non-heap.max',
            property: 'Value',
            key: 'Max',
            values: []
          },
          {
            name: 'metrics:name=jvm.memory.non-heap.committed',
            property: 'Value',
            key: 'Committed',
            values: []
          },
          {
            name: 'metrics:name=jvm.memory.non-heap.used',
            property: 'Value',
            key: 'Used',
            values: [],
            area: true
          }
        ]
      },
      {
        name: 'psEdenSpaceHeapMemoryUsage',
        label: 'Memory Pool "PS Eden Space"',
        xAxisTickFormat: $scope.dateFormat(),
        yAxisTickFormat: sizeFormat,
        values: [
          {
            name: 'java.lang:type=MemoryPool,name=[a-zA-Z ]*Eden Space',
            property: 'Usage/max',
            key: 'Max',
            values: []
          },
          {
            name: 'java.lang:type=MemoryPool,name=[a-zA-Z ]*Eden Space',
            property: 'Usage/committed',
            key: 'Committed',
            values: []
          },
          {
            name: 'java.lang:type=MemoryPool,name=[a-zA-Z ]*Eden Space',
            property: 'Usage/used',
            key: 'Used',
            values: [],
            area: true
          }
        ]
      },
      {
        name: 'psSurvivorSpaceHeapMemoryUsage',
        label: 'Memory Pool "PS Survivor Space"',
        xAxisTickFormat: $scope.dateFormat(),
        yAxisTickFormat: sizeFormat,
        values: [
          {
            name: 'java.lang:type=MemoryPool,name=[a-zA-Z ]*Survivor Space',
            property: 'Usage/max',
            key: 'Max',
            values: []
          },
          {
            name: 'java.lang:type=MemoryPool,name=[a-zA-Z ]*Survivor Space',
            property: 'Usage/committed',
            key: 'Committed',
            values: []
          },
          {
            name: 'java.lang:type=MemoryPool,name=[a-zA-Z ]*Survivor Space',
            property: 'Usage/used',
            key: 'Used',
            values: [],
            area: true
          }
        ]
      },
      {
        name: 'psOldGenHeapMemoryUsage',
        label: 'Memory Pool "PS Old Gen"',
        xAxisTickFormat: $scope.dateFormat(),
        yAxisTickFormat: sizeFormat,
        values: [
          {
            name: 'java.lang:type=MemoryPool,name=PS Old Gen|java.lang:type=MemoryPool,name=Tenured Gen',
            property: 'Usage/max',
            key: 'Max',
            values: []
          },
          {
            name: 'java.lang:type=MemoryPool,name=PS Old Gen|java.lang:type=MemoryPool,name=Tenured Gen',
            property: 'Usage/committed',
            key: 'Committed',
            values: []
          },
          {
            name: 'java.lang:type=MemoryPool,name=PS Old Gen|java.lang:type=MemoryPool,name=Tenured Gen',
            property: 'Usage/used',
            key: 'Used',
            values: [],
            area: true
          }
        ]
      },
      /*{
        name: 'psPermGenHeapMemoryUsage',
        label: 'Memory Pool "PS Perm Gen"',
        xAxisTickFormat: $scope.dateFormat(),
        yAxisTickFormat: sizeFormat,
        values: [
          {
            name: 'java.lang:type=MemoryPool,name=[a-zA-Z ]*Perm Gen',
            property: 'Usage/max',
            key: 'Max',
            values: []
          },
          {
            name: 'java.lang:type=MemoryPool,name=[a-zA-Z ]*Perm Gen',
            property: 'Usage/committed',
            key: 'Committed',
            values: []
          },
          {
            name: 'java.lang:type=MemoryPool,name=[a-zA-Z ]*Perm Gen',
            property: 'Usage/used',
            key: 'Used',
            values: []
          }
        ]
      },*/
      {
        name: 'psCodeCacheHeapMemoryUsage',
        label: 'Memory Pool "Code Cache"',
        xAxisTickFormat: $scope.dateFormat(),
        yAxisTickFormat: sizeFormat,
        values: [
          {
            name: 'java.lang:type=MemoryPool,name=Code Cache',
            property: 'Usage/max',
            key: 'Max',
            values: []
          },
          {
            name: 'java.lang:type=MemoryPool,name=Code Cache',
            property: 'Usage/committed',
            key: 'Committed',
            values: []
          },
          {
            name: 'java.lang:type=MemoryPool,name=Code Cache',
            property: 'Usage/used',
            key: 'Used',
            values: [],
            area: true
          }
        ]
      }
    ];

    if(!$rootScope.$storage.jvmMetricsChartList) {
      $rootScope.$storage.jvmMetricsChartList = _.pluck($scope.chartList, 'name');
    }

    /**
     * Fetch the JVM Metrics every Refresh Interval time.
     *
     */
    var refreshPipelineJMX = function() {

      jvmMetricsTimer = $timeout(
        function() {
        },
        configuration.getJVMMetricsRefreshInterval()
      );

      jvmMetricsTimer.then(
        function() {
          api.admin.getJMX()
            .success(function(data) {
              updateGraphData(data);
              refreshPipelineJMX();
            })
            .error(function(data, status, headers, config) {
              $rootScope.common.errors = [data];
            });
        },
        function() {
          console.log( "Timer rejected!" );
        }
      );
    };


    var updateGraphData = function(jmxData) {
      var chartList = $scope.chartList,
        date = (new Date()).getTime();

      angular.forEach(jmxData.beans, function(bean) {
        angular.forEach(chartList, function(chartObj) {

          angular.forEach(chartObj.values, function(chartBean) {
            //chartBean.area = true;
            var regExp = new RegExp(chartBean.name);
            if(regExp.test(bean.name)) {
              var propertyList = chartBean.property.split('/'),
                propertyValue = bean;

              angular.forEach(propertyList, function(property) {
                if(propertyValue) {
                  propertyValue = propertyValue[property];
                }
              });

              chartBean.values.push([date, propertyValue]);

              //Store only last 10 minutes data
              if(chartBean.values.length > 150) {
                chartBean.values.shift();
              }
            }
          });

          if(chartObj.displayProperties) {
            angular.forEach(chartObj.displayProperties, function(chartBean) {
              if(chartBean.name === bean.name) {
                var propertyList = chartBean.property.split('/'),
                  propertyValue = bean;

                angular.forEach(propertyList, function(property) {
                  if(propertyValue) {
                    propertyValue = propertyValue[property];
                  }
                });

                chartBean.value = propertyValue;
              }
            });
          }

        });
      });
    };

    api.admin.getJMX()
      .success(function(data) {
        updateGraphData(data);
        refreshPipelineJMX();
      })
      .error(function(data, status, headers, config) {
        $rootScope.common.errors = [data];
      });


    $scope.$on('$destroy', function(){
      $timeout.cancel(jvmMetricsTimer);
      destroyed = true;
    });

    $scope.$on('visibilityChange', function(event, isHidden) {
      if (isHidden) {
        $timeout.cancel(jvmMetricsTimer);
        destroyed = true;
      } else {
        refreshPipelineJMX();
        destroyed = false;
      }
    });

  });
