const GA4_DATABASE = 'molts-data-project';
const GA4_DATASET = 'analytics_218861058';
const GA4_TABLE = 'events_*';
const GA4_INTRADAY_TABLE = 'events_intraday_*';

//staging における一時エクスポートテーブル用
const GA4_DATASET_STAGING = 'df_molts_ga4_staging_new';
const GA4_TABLE_STAGING = 's_ga4_events_union';

module.exports = {GA4_DATABASE, GA4_DATASET, GA4_TABLE, GA4_INTRADAY_TABLE, GA4_DATASET_STAGING, GA4_TABLE_STAGING}