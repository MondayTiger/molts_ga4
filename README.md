# GA4->BigQuery export data gereral queries made by Dataform.

### イベントパラメータを追加する場合
以下のテーブル定義を更新する
- source
    - ga4_fixed_events.sqlx
    - ga4_unfixed_events_intraday.sqlx
    - ga4_unfixed_events.sqlx
- cleanse
    - c_ga4_fixed_events.sqlx
    - c_ga4_unfixed_events_intraday.sqlx
    - c_ga4_unfixed_events.sqlx
-staging
    - s_ga4_event.sqlx
    - s_ga4_events_add_session_item.sqlx
- mart
    - m_ga4_event.sqlx
    - m_ga4_session.sqlx *必要に応じ

### Data flow
```mermaid
erDiagram
		%% GA4のイベントデータからソースデータを構築
    t-analytics_PROPERTY_ID-events_yyyymmdd ||--|| v-source-ga4_fixed_events : ""
    t-analytics_PROPERTY_ID-events_yyyymmdd ||--|| v-source-ga4_unfixed_events : ""
    t-analytics_PROPERTY_ID-events_intraday_yyyymmdd ||--|| v-source-ga4_unfixed_events_intraday : ""

		%% ソースデータをクレンジング
    v-source-ga4_fixed_events ||--|| v-cleanse-c_ga4_fixed_events : ""
    v-source-ga4_unfixed_events ||--|| v-cleanse-c_ga4_unfixed_events : ""
    v-source-ga4_unfixed_events_intraday ||--|| v-cleanse-c_unfixed_events_intraday : ""

		%% クレンジングしたデータを統合&処理が複雑すぎて、後続の処理がBigQuery上で処理できないため、テーブル化
    v-cleanse-c_ga4_fixed_events ||--|| t-staging-s_ga4_events_union : "統合"
    v-cleanse-c_ga4_unfixed_events ||--|| t-staging-s_ga4_events_union : "統合"
    v-cleanse-c_unfixed_events_intraday ||--|| t-staging-s_ga4_events_union : "統合"

		%% 内部アクセスを除外
    t-staging-s_ga4_events_union_export ||--|| v-staging-s_ga4_events_exclude_internal : "debug_mode, traffic_type パラメータ, 不要ドメインを除外"

		%% イベント名を変更・削除・追加する場合の処理
    v-staging-s_ga4_events_exclude_internal ||--|| v-staging-s_ga4_events_event_update : "イベント名を変更・削除・追加する場合の処理"

		%% 統合データをもとにセッションデータを構築
    v-staging-s_ga4_events_event_update ||--|| v-staging-s_ga4_events_add_session_item : "統合データをもとにセッションデータを構築"

		%% 各データマートに追加する差分データを構築
    v-staging-s_ga4_events_add_session_item ||--|| v-staging-s_ga4_session : "セッションデータの差分データを作成"
    v-staging-s_ga4_events_add_session_item ||--|| v-staging-s_ga4_event : "イベントデータの差分データを作成"

		%% 蓄積済みの各データマートにデータを投入
    v-staging-s_ga4_session ||..|| t-mart-m_ga4_session : "セッションデータを追加"
    v-staging-s_ga4_event ||..|| t-mart-m_ga4_event : "イベントデータを追加"
 ```