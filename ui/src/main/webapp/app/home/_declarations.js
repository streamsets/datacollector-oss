/**
 * Home module for displaying home page content.
 */

angular
  .module('dataCollectorApp.home', [
    'ngRoute',
    'jsonFormatter',
    'splitterDirectives',
    'tabDirectives',
    'pipelineGraphDirectives',
    'dataCollectorApp.commonDirectives',
    'ui.bootstrap',
    'angularMoment',
    'nvd3ChartDirectives',
    'abbreviateNumberFilter',
    'ngSanitize',
    'ui.select',
    'showLoadingDirectives',
    'recordTreeDirectives',
    'ui.bootstrap.datetimepicker',
    'dataCollectorApp.codemirrorDirectives'
  ])
  .constant('amTimeAgoConfig', {
    withoutSuffix: true
  });