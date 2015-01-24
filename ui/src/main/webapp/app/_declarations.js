/**
 * Main Module
 */

angular.module('pipelineAgentApp', [
  'ngRoute',
  'ngCookies',
  'tmh.dynamicLocale',
  'pascalprecht.translate',
  'templates-app',
  'templates-common',
  'pipelineAgentApp.common',
  'pipelineAgentApp.home',
  'pipelineAgentApp.jvmMetrics',
  'pipelineAgentApp.logs',
  'ngStorage'
]);