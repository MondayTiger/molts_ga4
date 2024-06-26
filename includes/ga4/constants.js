const GA4_DATABASE = 'molts-data-project';
const GA4_DATASET = 'analytics_218861058';
const GA4_TABLE = 'events_*';
const GA4_INTRADAY_TABLE = 'events_intraday_*';
// 集計対象ホスト名 definitions/ga4/staging/s_ga4_events_exclude_internal.sqlxでホスト名の指定を行うが、ホスト名が複数必要な場合はここに追加し、s_ga4_events_exclude_internalにも追加すること
const HOSTNAME1 = 'moltsinc.co.jp';
const HOSTNAME2 = 'moltsinc.co.jp';  
// Measurement Protocolで使うイベント名 s_ga4_events_exclude_internal.sqlxでMeasurement Protocolで取得するイベント名の指定を行うが、ここにイベント名を記述。複数ある場合はここに追加し、s_ga4_events_exclude_internalにも追加すること
const MP_EVENT1 = 'Measurement Protocol用イベント1';
const MP_EVENT2 = 'Measurement Protocol用イベント2';
// report/r_ga4_conversion.sqlx 内のコンバージョンページURL
const CV_PAGE_LOCATION = 'https://moltsinc.co.jp/%thanks%';

module.exports = {GA4_DATABASE, GA4_DATASET, GA4_TABLE, GA4_INTRADAY_TABLE, HOSTNAME1, HOSTNAME2, MP_EVENT1, MP_EVENT2, CV_PAGE_LOCATION}