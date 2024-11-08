// GA4テーブル関連
const GA4_DATABASE = 'molts-data-project';
const GA4_DATASET = 'analytics_218861058';
const GA4_TABLE = 'events_*';   // 固定
const GA4_INTRADAY_TABLE = 'events_intraday_*';   // 固定

// 格納先データセット（要変更）
const PROJECT = 'molts-data-project';
const CLEANSE = 'df_molts_ga4_cleanse';
const MART = 'df_molts_ga4_mart';
const REPORT = 'df_molts_ga4_report';
const SOURCE = 'df_molts_ga4_source';
const STAGING = 'df_molts_ga4_staging';

// 計測関連
// 集計対象ホスト名 definitions/ga4/staging/s_ga4_events_event_update.sqlxでホスト名の指定を行うが、ホスト名が複数必要な場合はここに追加し、s_ga4_events_event_update.sqlxにも追加すること
const HOSTNAME1 = 'moltsinc.co.jp';
const HOSTNAME2 = 'moltsinc.co.jp';  
// Measurement Protocolで使うイベント名 s_ga4_events_exclude_internal.sqlxでMeasurement Protocolで取得するイベント名の指定を行うが、ここにイベント名を記述。複数ある場合はここに追加し、s_ga4_events_exclude_internalにも追加すること
const MP_EVENT1 = 'Measurement Protocol用イベント1';
const MP_EVENT2 = 'Measurement Protocol用イベント2';
// デフォルトチャネルグループのテーブル名（共通）　https://docs.google.com/spreadsheets/d/1gu6JfV0PD9QgfzPZT5EUCCvh1YHtENvpCENogSrxtd4/edit?gid=0#gid=0
const CHANNEL_GROUP_TABLE = 'molts-data-project.general_master_us.ga4_channel_grouping_base';

// GA4データの最も古い日
const GA4_FIRST_DATE = '2020-1-1';

// レポート関連 
// report/r_ga4_conversion.sqlx 内のコンバージョンページURL ※r_ga4_conversionテーブルに格納されるのはpage_view, screen_viewイベントのみ
const CV_PAGE_LOCATION = 'https://moltsinc.co.jp/%thanks%';
// コンバージョン分析ビュー（definitions/ga4/report/r_ga4_analysis_conversion.sqlx）などで使用する対象イベント
const GA4_ANALYSIS_CV_EVENTS = ['generate_lead','sign_up','download_form', 'contact_service_thanks','file_download'];



// columns オブジェクトを事前に作成
const GA4_ANALYSIS_CV_EVENTS_CONFIG = GA4_ANALYSIS_CV_EVENTS.map(val => `${val}: "${val}のイベント数"`).join(",\n");
const GA4_ANALYSIS_CV_START_DATE = "DATE('2024-09-01')";
const GA4_ANALYSIS_CV_END_DATE = "DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)" ;

module.exports = {GA4_DATABASE, GA4_DATASET, GA4_TABLE, GA4_INTRADAY_TABLE,PROJECT, CLEANSE, MART, REPORT ,SOURCE, STAGING, HOSTNAME1, HOSTNAME2,  MP_EVENT1, MP_EVENT2, CV_PAGE_LOCATION, CHANNEL_GROUP_TABLE,GA4_FIRST_DATE,GA4_ANALYSIS_CV_EVENTS,GA4_ANALYSIS_CV_EVENTS_CONFIG,GA4_ANALYSIS_CV_START_DATE,GA4_ANALYSIS_CV_END_DATE}