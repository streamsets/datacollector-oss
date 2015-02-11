/**
 * Service for providing access to the Pipeline utility functions.
 */
angular.module('dataCollectorApp.common')
  .service('pipelineService', function(pipelineConstant, $translate) {

    var self = this,
      translations = {};

    var getXPos = function(pipelineConfig, firstOpenLane) {
      var prevStage = (firstOpenLane && firstOpenLane.stageInstance) ? firstOpenLane.stageInstance :
        ((pipelineConfig.stages && pipelineConfig.stages.length) ? pipelineConfig.stages[pipelineConfig.stages.length - 1] : undefined);

      return prevStage ? prevStage.uiInfo.xPos + 220 : 60;
    };

    var getYPos = function(pipelineConfig, firstOpenLane, xPos) {
      var maxYPos = 0;

      if(firstOpenLane) {
        maxYPos = firstOpenLane.stageInstance.uiInfo.yPos - 150;
      }

      angular.forEach(pipelineConfig.stages, function(stage) {
        if(stage.uiInfo.xPos === xPos && stage.uiInfo.yPos > maxYPos) {
          maxYPos = stage.uiInfo.yPos;
        }
      });

      return maxYPos ? maxYPos + 150 : 50;
    };

    /**
     * Construct new instance for give Stage Definition
     * @param options
     *  stage
     *  pipelineConfig
     *  labelSuffix [Optional]
     *  firstOpenLane [Optional]
     *  relativeXPos [Optional]
     *  relativeYPos [Optional]
     *  configuration [Optional]
     * @returns {{instanceName: *, library: (*|stageInstance.library|library|e.library), stageName: *, stageVersion: *, configuration: Array, uiInfo: {label: *, description: string, xPos: *, yPos: number, stageType: *}, inputLanes: Array, outputLanes: Array}}
     */
    this.getNewStageInstance = function (options) {
      var stage = options.stage,
        pipelineConfig = options.pipelineConfig,
        labelSuffix = options.labelSuffix,
        firstOpenLane = options.firstOpenLane,
        relativeXPos = options.relativeXPos,
        relativeYPos = options.relativeYPos,
        configuration = options.configuration,
        xPos = relativeXPos || getXPos(pipelineConfig, firstOpenLane),
        yPos = relativeYPos || getYPos(pipelineConfig, firstOpenLane, xPos),
        stageLabel = self.getStageLabel(stage, pipelineConfig),
        stageInstance = {
          instanceName: stage.name + (new Date()).getTime() + (labelSuffix ? labelSuffix : ''),
          library: stage.library,
          stageName: stage.name,
          stageVersion: stage.version,
          configuration: [],
          uiInfo: {
            label: stageLabel,
            description: '', //stage.description,
            xPos: xPos,
            yPos: yPos,
            stageType: stage.type
          },
          inputLanes: [],
          outputLanes: []
        };

      if(firstOpenLane && firstOpenLane.laneName) {
        stageInstance.inputLanes.push(firstOpenLane.laneName);
      }

      if (stage.outputStreams > 0) {
        for(var i=0; i< stage.outputStreams; i++) {
          stageInstance.outputLanes.push(stageInstance.instanceName + 'OutputLane' + (new Date()).getTime() + i);
        }

        if(stage.outputStreams > 1) {
          stageInstance.uiInfo.outputStreamLabels = stage.outputStreamLabels;
        }
      } else if(stage.variableOutputStreams) {
        stageInstance.outputLanes.push(stageInstance.instanceName + 'OutputLane' + (new Date()).getTime());
      }

      angular.forEach(stage.configDefinitions, function (configDefinition) {
        stageInstance.configuration.push(self.setDefaultValueForConfig(configDefinition, stageInstance));
      });

      if(stage.rawSourceDefinition && stage.rawSourceDefinition.configDefinitions) {

        stageInstance.uiInfo.rawSource = {
          configuration: []
        };

        angular.forEach(stage.rawSourceDefinition.configDefinitions, function (configDefinition) {
          stageInstance.uiInfo.rawSource.configuration.push(self.setDefaultValueForConfig(configDefinition, stageInstance));
        });
      }

      stageInstance.uiInfo.icon = self.getStageIconURL(stage);

      if(configuration) {
        //Special handling for lanePredicates
        angular.forEach(configuration, function(config) {
          if(config.name === 'lanePredicates') {
            stageInstance.outputLanes = [];

            angular.forEach(config.value, function(lanePredicate, index) {
              var newOutputLane = stageInstance.instanceName + 'OutputLane' + (new Date()).getTime() + index;
              lanePredicate.outputLane = newOutputLane;
              stageInstance.outputLanes.push(newOutputLane);
            });
          }
        });

        stageInstance.configuration = configuration;
      }

      return stageInstance;
    };

    /**
     * Return Stage Label
     *
     * @param stage
     * @param pipelineConfig
     * @returns {*}
     */
    this.getStageLabel = function(stage, pipelineConfig) {
      var label = stage.label,
        similarStageInstances = _.filter(pipelineConfig.stages, function(stageInstance) {
          return stageInstance.uiInfo.label.indexOf(label) !== -1;
        });

      return label + (similarStageInstances.length + 1);
    };


    /**
     * Sets default value for config.
     *
     * @param configDefinition
     * @param stageInstance
     * @returns {{name: *, value: (defaultValue|*|string|Variable.defaultValue|undefined)}}
     */
    this.setDefaultValueForConfig = function(configDefinition, stageInstance) {
      var config = {
          name: configDefinition.name,
          value: configDefinition.defaultValue || undefined
        };

      if(configDefinition.type === 'MODEL') {
        if(configDefinition.model.modelType === 'FIELD_SELECTOR_MULTI_VALUED') {
          config.value = [];
        } else if(configDefinition.model.modelType === 'LANE_PREDICATE_MAPPING') {
          config.value = [{
            outputLane: stageInstance.outputLanes[0],
            predicate: ''
          }];
        } else if(configDefinition.model.modelType === 'COMPLEX_FIELD') {
          var complexFieldObj = {};
          angular.forEach(configDefinition.model.configDefinitions, function (complexFiledConfigDefinition) {
            var complexFieldConfig = self.setDefaultValueForConfig(complexFiledConfigDefinition, stageInstance);
            complexFieldObj[complexFieldConfig.name] = complexFieldConfig.value || '';
          });
          config.value = [complexFieldObj];
        }
      } else if(configDefinition.type === 'INTEGER') {
        if(config.value) {
          config.value = parseInt(config.value);
        } else {
          config.value = 0;
        }
      } else if(configDefinition.type === 'BOOLEAN' && config.value === undefined) {
        config.value = false;
      } else if(configDefinition.type === 'LIST') {
        config.value = [];
      } else if(configDefinition.type === 'MAP') {
        config.value = [];
      }

      return config;
    };

    /**
     * Return Stage Icon URL
     *
     * @param stage
     * @returns {string}
     */
    this.getStageIconURL = function(stage) {
      if(stage.icon) {
        return 'rest/v1/definitions/stages/icon?name=' + stage.name +
        '&library=' + stage.library + '&version=' + stage.version;
      } else {
        switch(stage.type) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            return 'assets/stage/defaultSource.svg';
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            return 'assets/stage/defaultProcessor.svg';
          case pipelineConstant.TARGET_STAGE_TYPE:
            return 'assets/stage/defaultTarget.svg';
        }
      }
    };

    $translate([
      'metrics.COUNTER_COUNT',

      //Related to Histogram
      'metrics.HISTOGRAM_COUNT',
      'metrics.HISTOGRAM_MAX',
      'metrics.HISTOGRAM_MIN',
      'metrics.HISTOGRAM_MEAN',
      'metrics.HISTOGRAM_MEDIAN',
      'metrics.HISTOGRAM_P50',
      'metrics.HISTOGRAM_P75',
      'metrics.HISTOGRAM_P95',
      'metrics.HISTOGRAM_P98',
      'metrics.HISTOGRAM_P99',
      'metrics.HISTOGRAM_P999',
      'metrics.HISTOGRAM_STD_DEV',

      //Meters
      'metrics.METER_COUNT',
      'metrics.METER_M1_RATE',
      'metrics.METER_M5_RATE',
      'metrics.METER_M15_RATE',
      'metrics.METER_M30_RATE',
      'metrics.METER_H1_RATE',
      'metrics.METER_H6_RATE',
      'metrics.METER_H12_RATE',
      'metrics.METER_H24_RATE',
      'metrics.METER_MEAN_RATE',

      //Timer
      'metrics.TIMER_COUNT',
      'metrics.TIMER_MAX',
      'metrics.TIMER_MIN',
      'metrics.TIMER_MEAN',
      'metrics.TIMER_P50',
      'metrics.TIMER_P75',
      'metrics.TIMER_P95',
      'metrics.TIMER_P98',
      'metrics.TIMER_P99',
      'metrics.TIMER_P999',
      'metrics.TIMER_STD_DEV',
      'metrics.TIMER_M1_RATE',
      'metrics.TIMER_M5_RATE',
      'metrics.TIMER_M15_RATE',
      'metrics.TIMER_MEAN_RATE'
    ]).then(function (_translations) {
      translations = _translations;
    });

    /**
     * Returns Metric element list
     */
    this.getMetricElementList = function() {
      var elementList = {
        COUNTER: [
          {
            value: 'COUNTER_COUNT',
            label: translations['metrics.COUNTER_COUNT']
          }
        ],
        HISTOGRAM: [
          {
            value: 'HISTOGRAM_COUNT',
            label: translations['metrics.HISTOGRAM_COUNT']
          },
          {
            value: 'HISTOGRAM_MAX',
            label: translations['metrics.HISTOGRAM_MAX']
          },
          {
            value: 'HISTOGRAM_MIN',
            label: translations['metrics.HISTOGRAM_MIN']
          },
          {
            value: 'HISTOGRAM_MEAN',
            label: translations['metrics.HISTOGRAM_MEAN']
          },
          {
            value: 'HISTOGRAM_MEDIAN',
            label: translations['metrics.HISTOGRAM_MEDIAN']
          },
          {
            value: 'HISTOGRAM_P50',
            label: translations['metrics.HISTOGRAM_P50']
          },
          {
            value: 'HISTOGRAM_P75',
            label: translations['metrics.HISTOGRAM_P75']
          },
          {
            value: 'HISTOGRAM_P95',
            label: translations['metrics.HISTOGRAM_P95']
          },
          {
            value: 'HISTOGRAM_P98',
            label: translations['metrics.HISTOGRAM_P98']
          },
          {
            value: 'HISTOGRAM_P99',
            label: translations['metrics.HISTOGRAM_P99']
          },
          {
            value: 'HISTOGRAM_P999',
            label: translations['metrics.HISTOGRAM_P999']
          },
          {
            value: 'HISTOGRAM_STD_DEV',
            label: translations['metrics.HISTOGRAM_STD_DEV']
          }
        ],
        METER: [
          {
            value: 'METER_COUNT',
            label: translations['metrics.METER_COUNT']
          },
          {
            value: 'METER_M1_RATE',
            label: translations['metrics.METER_M1_RATE']
          },
          {
            value: 'METER_M5_RATE',
            label: translations['metrics.METER_M5_RATE']
          },
          {
            value: 'METER_M15_RATE',
            label: translations['metrics.METER_M15_RATE']
          },
          {
            value: 'METER_M30_RATE',
            label: translations['metrics.METER_M30_RATE']
          },
          {
            value: 'METER_H1_RATE',
            label: translations['metrics.METER_H1_RATE']
          },
          {
            value: 'METER_H6_RATE',
            label: translations['metrics.METER_H6_RATE']
          },
          {
            value: 'METER_H12_RATE',
            label: translations['metrics.METER_H12_RATE']
          },
          {
            value: 'METER_H24_RATE',
            label: translations['metrics.METER_H24_RATE']
          },
          {
            value: 'METER_MEAN_RATE',
            label: translations['metrics.METER_MEAN_RATE']
          }
        ],
        TIMER: [
          {
            value: 'TIMER_COUNT',
            label: translations['metrics.TIMER_COUNT']
          },
          {
            value: 'TIMER_MAX',
            label: translations['metrics.TIMER_MAX']
          },
          {
            value: 'TIMER_MEAN',
            label: translations['metrics.TIMER_MEAN']
          },
          {
            value: 'TIMER_MIN',
            label: translations['metrics.TIMER_MIN']
          },
          {
            value: 'TIMER_P50',
            label: translations['metrics.TIMER_P50']
          },
          {
            value: 'TIMER_P75',
            label: translations['metrics.TIMER_P75']
          },
          {
            value: 'TIMER_P95',
            label: translations['metrics.TIMER_P95']
          },
          {
            value: 'TIMER_P98',
            label: translations['metrics.TIMER_P98']
          },
          {
            value: 'TIMER_P99',
            label: translations['metrics.TIMER_P99']
          },
          {
            value: 'TIMER_P999',
            label: translations['metrics.TIMER_P999']
          },
          {
            value: 'TIMER_STD_DEV',
            label: translations['metrics.TIMER_STD_DEV']
          },
          {
            value: 'TIMER_M1_RATE',
            label: translations['metrics.TIMER_M1_RATE']
          },
          {
            value: 'TIMER_M5_RATE',
            label: translations['metrics.TIMER_M5_RATE']
          },
          {
            value: 'TIMER_M15_RATE',
            label: translations['metrics.TIMER_M15_RATE']
          },
          {
            value: 'TIMER_MEAN_RATE',
            label: translations['metrics.TIMER_MEAN_RATE']
          }
        ]
      };

      return elementList;
    };


    /**
     * Returns metric element list for the given pipeline.
     *
     * @param pipelineConfig
     */
    this.getMetricIDList = function(pipelineConfig) {
      var metricIDList = {
        COUNTER: [],
        HISTOGRAM: [
            {
              value: 'pipeline.inputRecordsPerBatch.histogramM5',
              label: 'Pipeline Input Records Per Batch Histogram M5'
            },
            {
              value: 'pipeline.outputRecordsPerBatch.histogramM5',
              label: 'Pipeline Output Records Per Batch Histogram M5'
            },
            {
              value: 'pipeline.errorRecordsPerBatch.histogramM5',
              label: 'Pipeline Bad Records Per Batch Histogram M5'
            },
            {
              value: 'pipeline.errorsPerBatch.histogramM5',
              label: 'Pipeline Errors Per Batch Histogram M5'
            }
          ],
        METER: [
            {
              value: 'pipeline.batchCount.meter',
              label: 'Pipeline Batch Count Meter'
            },
            {
              value: 'pipeline.batchInputRecords.meter',
              label: 'Pipeline Batch Input Records Meter'
            },
            {
              value: 'pipeline.batchOutputRecords.meter',
              label: 'Pipeline Batch Output Records Meter '
            },
            {
              value: 'pipeline.batchErrorRecords.meter',
              label: 'Pipeline Batch Bad Records Meter'
            },
            {
              value: 'pipeline.batchErrorMessages.meter',
              label: 'Pipeline Batch Error Messages Meter'
            }
          ],
        TIMER: [{
            value: 'pipeline.batchProcessing.timer',
            label: 'Pipeline Batch Processing Timer'
          }]
        };



      angular.forEach(pipelineConfig.stages, function(stage) {
        var instanceName = stage.instanceName,
          label = stage.uiInfo.label;

        //Counters
        metricIDList.COUNTER.push.apply(metricIDList.COUNTER, [
          {
            value: 'stage.' + instanceName + '.inputRecords.counter',
            label: label + ' Input Records Counter'
          },
          {
            value: 'stage.' + instanceName + '.outputRecords.counter',
            label: label + ' Output Records Counter'
          },
          {
            value: 'stage.' + instanceName + '.errorRecords.counter',
            label: label + ' Bad Records Counter'
          },
          {
            value: 'stage.' + instanceName + '.stageErrors.counter',
            label: label + ' Stage Errors Counter'
          }
        ]);

        //histograms
        metricIDList.HISTOGRAM.push.apply(metricIDList.HISTOGRAM, [
          {
            value: 'stage.' + instanceName + '.inputRecords.histogramM5',
            label: label + ' Input Records Histogram'
          },
          {
            value: 'stage.' + instanceName + '.outputRecords.histogramM5',
            label: label + ' Output Records Histogram'
          },
          {
            value: 'stage.' + instanceName + '.errorRecords.histogramM5',
            label: label + ' Error Records Histogram'
          },
          {
            value: 'stage.' + instanceName + '.stageErrors.histogramM5',
            label: label + ' Stage Errors Histogram'
          }
        ]);

        //meters
        metricIDList.METER.push.apply(metricIDList.METER, [
          {
            value: 'stage.' + instanceName + '.inputRecords.meter',
            label: label + ' Input Records Meter'
          },
          {
            value: 'stage.' + instanceName + '.outputRecords.meter',
            label: label + ' Output Records Meter'
          },
          {
            value: 'stage.' + instanceName + '.errorRecords.meter',
            label: label + ' Error Records Meter'
          },
          {
            value: 'stage.' + instanceName + '.stageErrors.meter',
            label: label + ' Stage Errors Meter'
          }
        ]);


        metricIDList.TIMER.push({
          value: 'stage.' + instanceName + '.batchProcessing.timer',
          label: label + ' Batch Processing Timer'
        });

      });

      return metricIDList;
    };


    /**
     * Return Pipeline and lane triggered alerts.
     *
     * @param pipelineRules
     * @param pipelineMetrics
     */
    this.getTriggeredAlerts = function(pipelineRules, pipelineMetrics) {
      var gauges = pipelineMetrics.gauges,
        alerts = [];

      angular.forEach(pipelineRules.metricsRuleDefinitions, function(rule) {
        var gaugeName = 'alert.' + rule.id + '.gauge';
        if(gauges[gaugeName]) {
          alerts.push({
            rule: rule,
            gauge: gauges[gaugeName],
            type: 'METRIC_ALERT'
          });
        }
      });

      angular.forEach(pipelineRules.dataRuleDefinitions, function(rule) {
        var gaugeName = 'alert.' + rule.id + '.gauge';
        if(gauges[gaugeName]) {
          alerts.push({
            rule: rule,
            gauge: gauges[gaugeName],
            type: 'DATA_ALERT'
          });
        }
      });

      return alerts;
    };


    this.getPredefinedMetricAlertRules = function(pipelineName) {
      return [
        {
          id: pipelineName + 'badRecords' + (new Date()).getTime(),
          alertText: "High incidence of Bad Records",
          metricId: "pipeline.batchErrorRecords.meter",
          metricType: "METER",
          metricElement: "METER_COUNT",
          condition: "${value() > 100}",
          enabled: false,
          sendEmail: false,
          valid: true
        },
        {
          id: pipelineName + 'stageErrors' + (new Date()).getTime(),
          alertText: "High incidence of Error Messages",
          metricId: "pipeline.batchErrorMessages.meter",
          metricType: "METER",
          metricElement: "METER_COUNT",
          condition: "${value() > 100}",
          enabled: false,
          sendEmail: false,
          valid: true
        }
      ];
    };

  });