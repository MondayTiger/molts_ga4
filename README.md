# GA4->BigQuery export data gereral queries made by Dataform.

# 各SQLXファイルの説明

| SQLXファイル名 | 対象 |
| ------------- | ------------- |
| fixed_events.sqlx | クエリ実行の5～7日前のデータ(テーブル)が対象 |
| unfixed_events.sqlx | クエリ実行の前日～4日前のデータが対象 |
| unfixed_events_intraday.sqlx | クエリ実行の当日のデータが対象 |


# ディレクトリ構成
- definitions/ga4/
    - source
       - ga4_fixed_events.sqlx
       - ga4_unfixed_events_intraday.sqlx
       - ga4_unfixed_events.sqlx    
    - cleanse
       - c_ga4_fixed_events.sqlx
       - c_ga4_unfixed_events_intraday.sqlx
       - c_ga4_unfixed_events.sqlx
    - staging
       - s_ga4_events_exclude_internal.sqlx
       - s_ga4_events_add_session_item.sqlx
       - s_ga4_event.sqlx
       - s_ga4_session.sqlx
    - mart
       - m_ga4_event_delete_unfixed.sqlx
       - m_ga4_event.sqlx
       - m_ga4_session_delete_unfixed.sqlx
       - m_ga4_session.sqlx

- includes/
    - constatns.js : GCPプロジェクト名、対象ホスト名などの定数をまとめたファイル
    - helpers.js : SQLXを簡略化するための関数が入ったファイル 


## Data flow
```mermaid
erDiagram
		%% GA4のイベントデータからソースデータを構築
    t-analytics_PROPERTY_ID-events_yyyymmdd ||--|| source-ga4_fixed_events : ""
    t-analytics_PROPERTY_ID-events_yyyymmdd ||--|| source-ga4_unfixed_events : ""
    t-analytics_PROPERTY_ID-events_intraday_yyyymmdd ||--|| source-ga4_unfixed_events_intraday : ""

		%% ソースデータをクレンジング
    source-ga4_fixed_events ||--|| cleanse-c_ga4_fixed_events : ""
    source-ga4_unfixed_events ||--|| cleanse-c_ga4_unfixed_events : ""
    source-ga4_unfixed_events_intraday ||--|| cleanse-c_unfixed_events_intraday : ""

		%% クレンジングしたデータを統合&処理が複雑すぎて、後続の処理がBigQuery上で処理できないため、テーブル化
    cleanse-c_ga4_fixed_events ||--|| staging-s_ga4_events_union : "統合＆テーブル化"
    cleanse-c_ga4_unfixed_events ||--|| staging-s_ga4_events_union : "統合＆テーブル化"
    cleanse-c_unfixed_events_intraday ||--|| staging-s_ga4_events_union : "統合＆テーブル化"

		%% 内部アクセスを除外
    staging-s_ga4_events_union ||--|| staging-s_ga4_events_exclude_internal : "debug_mode, traffic_type パラメータ, 不要ドメインを除外"

		%% イベント名を変更・削除・追加する場合の処理
    staging-s_ga4_events_exclude_internal ||--|| staging-s_ga4_events_event_update : "イベント名を変更・削除・追加する場合の処理"

		%% 統合データをもとにセッションデータを構築
    staging-s_ga4_events_event_update ||--|| staging-s_ga4_events_add_session_item : "統合データをもとにセッションデータを構築"

		%% 各データマートに追加する差分データを構築
    staging-s_ga4_events_add_session_item ||--|| staging-s_ga4_session : "セッションデータの差分データを作成"
    staging-s_ga4_events_add_session_item ||--|| staging-s_ga4_event : "イベントデータの差分データを作成"

		%% 蓄積済みの各データマートにデータを投入
    staging-s_ga4_session ||..|| mart-m_ga4_session : "セッションデータを追加"
    staging-s_ga4_event ||..|| mart-m_ga4_event : "イベントデータを追加"
 ```


# 処理の流れ
1. GA4からエクスポートされたテーブル（analytics_xxxxxxx.events_YYYYMMDD, analytics_xxxxxxx.events_intraday_YYYYMMDD）内のevent_paramsカラムなどをフラット化
   - definitions/ga4/sourceディレクトリ内のSQLXファイル
2. 上記で生成されたデータ（ビューの結果）に対して、下記を実施
   - definitions/ga4/cleanseディレクトリ内のSQLXファイル
     * イベントの発生時刻を日本時間に変更
     * URLからパラメータを除去したカラムを追加
     * link URLからパラメータを除去したカラムを追加
3. 上記で生成されたデータ（ビューの結果）を1つのテーブル（xxx_ga4_staging.s_ga4_events_union）にまとめる
   - definitions/ga4/staging/s_ga4_events_union.sqlx
4. イベントを除外
   - definitions/ga4/staging/s_ga4_events_exclude_internal.sqlx
     * デバッグモードや内部アクセスのイベントを除外
     * 計測対象のホスト名を指定（指定していないホスト名は除外される）
     * Measurement Protocol用のイベントがある場合は含める
5. イベントデータを加工する必要がればここで実施（今回は何もしていません）
   - definitions/ga4/staging/s_ga4_events_event_update.sqlx
6. セッション情報を追加
   - definitions/ga4/staging/s_ga4_events_add_session_item.sqlx
     * セッションの参照元情報
     * セッション時間
     * セッションごとのページビュー（スクリーンビュー）数
7. 各page_view/screen_viewイベントに関する下記のデータを追加
   - definitions/ga4/staging/s_ga4_event.sqlx
     * 前後3ページ（スクリーン）のパス
     * ページ滞在時間
     * ランディングページを判定するフラグ
     * 直帰、離脱を判定するフラグ
8. 上記7で作成したイベントデータを gcp-ist-dxp.df_ist_dxp_ga4_mart.m_ga4_event に格納
   - definitions/ga4/mart/m_ga4_event_delete_unfixed.sqlx
     直近分のデータを削除
   - definitions/ga4/mart/m_ga4_event.sqlx
     今回作成した7の結果を追加
9. 上記6のデータ（ビュー）からセッション単位のデータを作成
   - definitions/ga4/staging/s_ga4_session.sqlx
10. 上記9で作成したセッションデータを ga4_mart.m_ga4_session に格納
    - definitions/ga4/mart/m_ga4_session_delete_unfixed.sqlx
      直近分のデータを削除
    - definitions/ga4/mart/m_ga4_session.sqlx
      今回作成した7の結果を追加
11. コンバージョンデータを ga4_report.r_ga4_conversion に格納
    - definitions/ga4/report/r_ga4_conversion.sqlx
      * 今回はthanksを含むページに到達したイベントを対象
      * 設定箇所：includes/constants.js内で設定したCV_PAGE_LOCATION = 'https://https://moltsinc.co.jp/%thanks%';

## 各セッションの参照元・メディア・キャンペーンなどの取得手順
1. 各イベントのcollected_traffic_source.manual_source（ない場合はevent_params内のsource）を取得。メディアやキャンペーンなども同様。※collected_traffic_sourceカラムは2023年中頃から追加されたため、それ以前の場合は下記ファイルのコメントアウト箇所を要変更。
   - 対象クエリ
    - source.ga4_fixed_events.sqlx
    - source.ga4_unfixed_events_intraday.sqlx
    - source.ga4_unfixed_events.sqlx
2. session_startイベントから上記を取得
3. 各セッションで上記1が存在する最も古いイベントから取得
4. session_startイベントに参照元が入っていれば（上記2）それを採用し、入っていない場合はイベント（上記3）から取得
   - 上記2以降の対象クエリ
     - staging.ga4_unfixed_events.sqlx


# 変更が必要な箇所
## includes/constants.js内
1. BigQuery関連
2. 集計対象ホスト名: クロスドメイントラッキングなどで複数のホスト名が集計対象となる場合は、HOSTNAME3など追加し、module.exports配列に追加
3. Measurement Protocolのイベント名 
4. コンバージョン対象: 現在はCV_PAGE_LOCATIONとしていますが、report/r_ga4_conversion.sqlx含め要変更
5. 新たに定数を作成したい場合は、このファイルで作成し、module.exports配列に追加

## イベントパラメータを追加した場合
基本的にはすべてのファイルで修正が必要
1. source/s_ga4_fixed_events.sqlx など
    event_paramsカラムから対象のパラメータを抽出
    例 114行目: ${helpers.getEventParamAll('event_category','string')}
2. cleanse/c_ga4_fixed_events.sqlx など
    上記で抽出したカラムを追加
3. staging/やmart/内のSQLXファイルも上記と同様

## 初回実行時の対応
下記のクエリのWHERE句部分を削除(コメントアウト)
* definitions/ga4/mart/m_ga4_session.sqlx
* definitions/ga4/mart/m_ga4_event.sqlx

# その他、設定変更が必要な場合に変更すべきファイル
1. GA4からエクスポートされたテーブル内のデータを利用したい場合: definitions/ga4/sourceディレクトリ内のSQLXファイル
2. すでに作成されたカラムを加工したい場合、staging/s_ga4_events_event_update.sqlx
3. 参照元情報などセッションスコープのデータを加工したい場合、staging/s_ga4_session.sqlx

