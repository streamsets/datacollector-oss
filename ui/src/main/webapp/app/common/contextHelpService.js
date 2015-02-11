/**
 * Service for providing access to the Configuration from dist/src/main/etc/pipeline.properties.
 */
angular.module('dataCollectorApp.common')
  .service('contextHelpService', function($q, configuration) {
    var self = this,
      helpIds = {
        'errors': 'index.html#/ViewingPipelineStageStatistics.dita#task_pkh_j23_tq',
        'dataRules': 'index.html#Pipeline_Monitoring/DataAlerts.html',
        'metricAlerts' : 'index.html#Pipeline_Monitoring/MetricAlerts.html',
        'monitoring' : 'index.html#Pipeline_Monitoring/PipelineMonitoring.html#concept_hsp_tnt_lq',
        'pipelineConfiguration' :   'index.html#Pipeline_Configuration/ConfiguringAPipeline.html',
        'preview' : 'index.html#Data_Preview/PreviewingaSingleStage.html#task_cxd_p25_qq',
        'snapshot' : 'index.html#Pipeline_Monitoring/ReviewingSnapshotData.html',
        'streamsets-datacollector-apache-kafka_0_8_1_1-lib-com_streamsets_pipeline_lib_kafka_HighLevelKafkaSource':  'index.html#Origins/KConsumer.html#task_npx_xgf_vq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_source_logtail_FileTailSource':  'index.html#Origins/FileTail.html#task_unq_wdw_yq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_source_spooldir_SpoolDirSource' : 'index.html#Origins/Directory.html#task_gfj_ssv_yq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_processor_dedup_DeDupProcessor' : 'index.html#Processors/RDeduplicator-Configuring.html#task_ikr_c2f_zq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_processor_fieldhasher_FieldHasherProcessor' : 'index.html#Processors/FieldHasher.html#task_xjd_dlk_wq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_processor_fieldmask_FieldMaskProcessor' : 'index.html#Processors/FieldMasker.html#task_vgg_z44_wq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_processor_jsonparser_JsonParserProcessor' : 'index.html#Processors/JSONParser.html#task_kwz_lg2_zq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_processor_fieldtypeconverter_FieldTypeConverterProcessor' : 'index.html#Processors/FieldConverter.html#task_g23_2tq_wq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_processor_fieldvaluereplacer_FieldValueReplacer' : 'index.html#Processors/ValueReplacer.html#task_ihq_ymf_zq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_processor_expression_ExpressionProcessor' : 'index.html#Processors/Expression.html#task_x2h_tv4_yq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_processor_fieldfilter_FieldFilterProcessor' : 'index.html#Processors/FieldRemover.html#task_c1j_btr_wq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_processor_splitter_SplitterProcessor' : 'index.html#Processors/FieldSplitter.html#task_av1_5g3_yq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_processor_selector_SelectorProcessor' : 'index.html#Processors/StreamSelector.html#task_iss_2zx_wq',
        'streamsets-datacollector-apache-kafka_0_8_1_1-lib-com_streamsets_pipeline_lib_kafka_KafkaTarget' : 'index.html#Destinations/KProducer-Configuring.dita#task_q4d_4yl_zq',
        'streamsets-datacollector-basic-lib-com_streamsets_pipeline_lib_stage_destination_NullTarget' : 'index.html#Destinations/Trash.html#task_ad4_qyl_zq',
        'streamsets-datacollector-cdh5_3_0-lib-com_streamsets_pipeline_hdfs_HdfsTarget' : 'index.html#Destinations/HadoopFS-Configuring.dita#task_m2m_skm_zq'
      };

    this.configInitPromise = configuration.init();

    this.launchHelp = function(helpId) {
      this.configInitPromise.then(function() {
        var relativeURL = helpIds[helpId],
          uiHelpBaseURL = configuration.getUIHelpBaseURL(),
          helpURL = uiHelpBaseURL + '/' + (relativeURL || 'index.html');
        window.open(helpURL);
      });
    };

  });