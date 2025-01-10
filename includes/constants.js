// 初期導入判定フラグ
const INITIALIZATION = false // 最初に導入する場合はtrue,それ以外はfalse

// BigQuery関連
// データマート格納先プロジェクト＆データセット
const PROJECT = 'molts-data-project';
const CLEANSE = 'df_molts_ga4_cleanse';
const MART = 'df_molts_ga4_mart';
const REPORT = 'df_molts_ga4_report';
const SOURCE = 'df_molts_ga4_source';
const STAGING = 'df_molts_ga4_staging';

// GA4テーブルがあるプロジェクト＆データセット
const GA4_DATABASE = 'molts-data-project'; // GAテーブルが格納されているプロジェクト名
const GA4_DATASET = 'analytics_218861058'; // GAテーブルが格納されているデータセット名
const GA4_TABLE = 'events_*';   // 固定
const GA4_INTRADAY_TABLE = 'events_intraday_*';   // 固定

// GA4データがBigQueryにエクスポートされた最も古い日
const GA4_FIRST_DATE = '2020-1-1';

// GA関連
// 集計対象ホスト名 （definitions/ga4/staging/s_ga4_events_event_update.sqlxでホスト名の指定を行う）
const HOSTNAME = ['moltsinc.co.jp','moltsinc2.co.jp','moltsinc3.co.jp']; // 完全一致で指定したい場合はこちら ※空にしたい場合は ['']とすること
const HOSTNAME_LIKE= '%molts%' // 条件の指定方法が hostname LIKE '%molts%'  ※空にしたい場合は ''とすること
// 抽出したいカスタムイベントパラメータとその型を入力 ※1度Dataformを走らせてから実行してから追加する場合は m_ga4_eventテーブルへ書き込み時にエラーとなるのでm_ga4_eventテーブルを手動で修正する必要あり
const EVENT_PARAMS = [  
    // {'article_tag':'string'},
    // {'click_area_click':'string'},
    // {'click_area_pv':'string'},
    // {'description':'string'},
    // {'directory1':'string'},
    // {'directory2':'string'},
    // {'directory3':'string'},
    // {'error_url':'string'},
    // {'ignore_referrer':'string'},
    // {'service_page_cta_count':'int'},
    // {'synthetic_bundle':'int'},
    // {'client_timestamp':'int'},
    // {'contact_cta_count':'int'},
    // {'download_cta_count':'int'},
    // {'error_count':'int'},
    // {'error_line':'int'}
]
// 抽出したいカスタムのユーザープロパティとその型
const USER_PROPERTIES = [
//   {'user_id':'string'}
]

// Measurement Protocolで使うイベント名 s_ga4_events_exclude_internal.sqlxでMeasurement Protocolで取得するイベント名の指定を行うが、ここにイベント名を記述。複数ある場合はここに追加し、s_ga4_events_exclude_internalにも追加すること
const MP_EVENT = ['Measurement Protocol用イベント1','Measurement Protocol用イベント2'] // 条件の指定方法が event_name IN( 'Measurement Protocol用イベント1','Measurement Protocol用イベント2')
const MP_EVENT_LIKE = '%Measurement Protocol用イベント1%';    // 条件の指定方法が event_name LIKE '%Measurement Protocol用イベント1%'
// デフォルトチャネルグループのテーブル名（共通）　https://docs.google.com/spreadsheets/d/1gu6JfV0PD9QgfzPZT5EUCCvh1YHtENvpCENogSrxtd4/edit?gid=0#gid=0
const CHANNEL_GROUP_TABLE = 'molts-data-project.general_master_us.ga4_channel_grouping_base';

// definitions/ga4/mart/m_ga4_session_channel_group.sqlxの118行目以降のlp_group, domainを手動で設定

// レポート関連 
// report/r_ga4_conversion.sqlx 内のコンバージョンページURL ※r_ga4_conversionテーブルに格納されるのはpage_view, screen_viewイベントのみ
const CV_PAGE_LOCATION_LIKE = 'https://moltsinc.co.jp/%thanks%';    // 部分一致
const CV_PAGE_LOCATION = ['https://moltsinc.co.jp/conversion','https://moltsinc.co.jp/complete']    // 完全一致
// コンバージョン分析ビュー（definitions/ga4/report/r_ga4_analysis_conversion.sqlx）などで使用する対象イベント
const GA4_ANALYSIS_CV_EVENTS = ['generate_lead','sign_up','download_form', 'contact_service_thanks','file_download'];
// 分析ビューの対象期間の開始日と終了日
const GA4_ANALYSIS_CV_START_DATE = "DATE('2024-09-01')";    // 開始日
const GA4_ANALYSIS_CV_END_DATE = "CURRENT_DATE('Asia/Tokyo')" ; // 終了日 ※今日


// 以下、日次更新関連 基本的には変更不要
const GA4_EVENTS_DATE_RANGE = 10; // 10日前 // events_テーブルの更新対象期間（通常は10日～前日）DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 10 DAY)   // 9日前まで分が通常更新されますが、前日にエラーがあった場合などを想定して1日余分に抽出します。
const GA4_EVENTS_DEFAULT_START_DATE =  "DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL " + GA4_EVENTS_DATE_RANGE + " DAY)";
const GA4_EVENTS_START_DATE = INITIALIZATION ? "'" + GA4_FIRST_DATE + "'" : "DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL " + GA4_EVENTS_DATE_RANGE + " DAY)";
const GA4_EVENTS_END_DATE = "DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)";  // 前日※こちらは変更する事はまずない
const MART_TYPE = INITIALIZATION ? "table":"incremental";    // m_ga4_eventなどのtypeの設定。初期導入時ならtable、日次更新ならincremental

// columns オブジェクトを事前に作成
const GA4_ANALYSIS_CV_EVENTS_CONFIG = GA4_ANALYSIS_CV_EVENTS.map(val => `${val}: "${val}のイベント数"`).join(",\n");
const HOSTNAMES = HOSTNAME.map(val => `'${val}'`).join(", ");
const MP_EVENTS = MP_EVENT.map(val => `'${val}'`).join(", ");
const CV_PAGE_LOCATIONS = CV_PAGE_LOCATION.map(val => `'${val}'`).join(", ");

module.exports = {GA4_DATABASE, GA4_DATASET, GA4_TABLE, GA4_INTRADAY_TABLE, GA4_EVENTS_DATE_RANGE,
    PROJECT, CLEANSE, MART, REPORT ,SOURCE, STAGING, 
    HOSTNAMES, HOSTNAME_LIKE, EVENT_PARAMS, USER_PROPERTIES,
    MP_EVENTS, MP_EVENT_LIKE,
    CHANNEL_GROUP_TABLE,
    CV_PAGE_LOCATION_LIKE,CV_PAGE_LOCATIONS, GA4_ANALYSIS_CV_EVENTS,
    INITIALIZATION, GA4_FIRST_DATE,
    GA4_EVENTS_DEFAULT_START_DATE,
    GA4_EVENTS_START_DATE,GA4_EVENTS_END_DATE,MART_TYPE,
    GA4_ANALYSIS_CV_EVENTS_CONFIG,GA4_ANALYSIS_CV_START_DATE,GA4_ANALYSIS_CV_END_DATE};
