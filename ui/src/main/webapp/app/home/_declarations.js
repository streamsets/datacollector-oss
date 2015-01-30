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
    'underscore',
    'ui.bootstrap',
    'angularMoment',
    'nvd3ChartDirectives',
    'abbreviateNumberFilter',
    'ngSanitize',
    'ui.select',
    'showLoadingDirectives',
    'recordTreeDirectives'
  ])
  .constant('amTimeAgoConfig', {
    withoutSuffix: true
  });