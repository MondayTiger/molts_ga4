/**********************************************
URLパラメータ（クエリストリング） の処理用
**********************************************/

// パラメータの値を抽出

const getQueryString = (url, queryString, columnName = false) => {
  return `SAFE_CAST(REGEXP_EXTRACT(${url}, r'^.*${queryString}=([^&]*)&\\?.*') AS STRING) AS ${
    columnName ? columnName : queryString
  }`;
};


// パラメータを削除

const deleteQueryString = (url, columnName = false) => {
  return `REGEXP_REPLACE(${url},r'\\?.*','') AS ${
    columnName ? columnName : queryString
  }`;
};



/**********************************************
パス分析
**********************************************/

// event_timestampに沿って連番を付与
const setNumber = (columnName) => {
  return `ROW_NUMBER() OVER(PARTITION BY user_pseudo_id, ga_session_id ORDER BY event_timestamp) AS ${columnName}`;
};


// セッション内の最初の値を返す

const getFirstValue = (value, columnName) => {
  return `FIRST_VALUE(${value} IGNORE NULLS) OVER(PARTITION BY user_pseudo_id, ga_session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ${columnName}`;
};


// セッション内の最初の値を返す（意図的にカラム名を返さない）

const getFirstValueNoColumnName = (value) => {
  return `FIRST_VALUE(${value} IGNORE NULLS) OVER(PARTITION BY user_pseudo_id, ga_session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`;
};


// セッション内の最後の値を返す

const getLastValue = (value, columnName) => {
  return `LAST_VALUE(${value} IGNORE NULLS) OVER(PARTITION BY user_pseudo_id, ga_session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ${columnName}`;
};


// セッション内の最後の値を返す（意図的にカラム名を返さない）

const getLastValueNoColumnName = (value) => {
  return `LAST_VALUE(${value} IGNORE NULLS) OVER(PARTITION BY user_pseudo_id, ga_session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`;
};


// 前のパスを返す
const getPreviousPath = (value, depth, columnName) => {
  return `LAG(${value}, ${depth}) OVER(PARTITION BY user_pseudo_id, ga_session_id ORDER BY event_timestamp) AS ${columnName}`;
};


// 次のパスを返す
const getNextPath = (value, depth, columnName) => {
  return `LEAD(${value}, ${depth}) OVER(PARTITION BY user_pseudo_id, ga_session_id ORDER BY event_timestamp) AS ${columnName}`;
};



/**********************************************
event_params の抽出用
**********************************************/

// event_params を抽出

const getEventParam = (eventParamName, eventParamType = "string", columnName = false) => {
  let eventParamTypeName = "";
  switch (eventParamType) {
    case "string":
      eventParamTypeName = "string_value";
      break;
    case "int":
      eventParamTypeName = "int_value";
      break;
    case "double":
      eventParamTypeName = "double_value";
      break;
    default:
      throw "eventType is not valid";
  }
  return `(SELECT value.${eventParamTypeName} AS ${eventParamName} FROM UNNEST(event_params) WHERE key = '${eventParamName}') AS ${
    columnName ? columnName : eventParamName
  }`;
};


// event_params を抽出（意図的にカラム名を返さない）

const getEventParamNoColumnName = (eventParamName, eventParamType = "string", columnName = false) => {
  let eventParamTypeName = "";
  switch (eventParamType) {
    case "string":
      eventParamTypeName = "string_value";
      break;
    case "int":
      eventParamTypeName = "int_value";
      break;
    case "double":
      eventParamTypeName = "double_value";
      break;
    default:
      throw "eventType is not valid";
  }
  return `(SELECT value.${eventParamTypeName} AS ${eventParamName} FROM UNNEST(event_params) WHERE key = '${eventParamName}')`;
};


// event_params を抽出（型がどれで入っているか不明な場合）

const getEventParamAll = (eventParamName, eventParamType = "string", columnName = false) => {
  switch (eventParamType) {
    case "string":
      return `(SELECT COALESCE(value.string_value, SAFE_CAST(value.int_value AS STRING), SAFE_CAST(value.double_value AS STRING)) FROM UNNEST(event_params) WHERE key = '${eventParamName}') AS ${
        columnName ? columnName : eventParamName
      }`;
      break;
    case "int":
      return `(SELECT COALESCE(value.int_value, SAFE_CAST(value.double_value AS INTEGER), SAFE_CAST(value.string_value AS INTEGER)) FROM UNNEST(event_params) WHERE key = '${eventParamName}') AS ${
        columnName ? columnName : eventParamName
      }`;
      break;
    case "double":
      return `(SELECT COALESCE(value.double_value, SAFE_CAST(value.int_value AS FLOAT64), SAFE_CAST(value.string_value AS FLOAT64)) FROM UNNEST(event_params) WHERE key = '${eventParamName}') AS ${
        columnName ? columnName : eventParamName
      }`;
      break;
    default:
      throw "eventType is not valid";
  }
};



/**********************************************
user_property の抽出用
**********************************************/

// user_property を抽出

const getUserProperty = (userPropertyName, userPropertyType = "string", columnName = false) => {
  let userPropertyTypeName = "";
  switch (userPropertyType) {
    case "string":
      userPropertyTypeName = "string_value";
      break;
    case "int":
      userPropertyTypeName = "int_value";
      break;
    case "double":
      userPropertyTypeName = "double_value";
      break;
    default:
      throw "userPropertyType is not valid";
  }
  return `(SELECT value.${userPropertyTypeName} AS ${userPropertyName} FROM UNNEST(user_properties) WHERE key = '${userPropertyName}') AS ${
    columnName ? columnName : userPropertyName
  }`;
};


// user_property を抽出（意図的にカラム名を返さない）

const getUserPropertyNoColumnName = (userPropertyName, userPropertyType = "string", columnName = false) => {
  let userPropertyTypeName = "";
  switch (userPropertyType) {
    case "string":
      userPropertyTypeName = "string_value";
      break;
    case "int":
      userPropertyTypeName = "int_value";
      break;
    case "double":
      userPropertyTypeName = "double_value";
      break;
    default:
      throw "userPropertyType is not valid";
  }
  return `(SELECT value.${userPropertyTypeName} AS ${userPropertyName} FROM UNNEST(user_properties) WHERE key = '${userPropertyName}')`;
};


// user_property を抽出（型がどれで入っているか不明な場合）

const getUserPropertyAll = (userPropetyName, userPropertyType = "string", columnName = false) => {
  switch (userPropertyType) {
    case "string":
      return `(SELECT COALESCE(value.string_value, SAFE_CAST(value.int_value AS STRING), SAFE_CAST(value.double_value AS STRING)) FROM UNNEST(user_properties) WHERE key = '${userPropertyName}') AS ${
        columnName ? columnName : userPropetyName
      }`;
      break;
    case "int":
      return `(SELECT COALESCE(value.int_value, SAFE_CAST(value.double_value AS INTEGER), SAFE_CAST(value.string_value AS INTEGER)) FROM UNNEST(user_properties) WHERE key = '${userPropertyName}') AS ${
        columnName ? columnName : userPropetyName
      }`;
      break;
    case "double":
      return `(SELECT COALESCE(value.double_value, SAFE_CAST(value.int_value AS FLOAT64), SAFE_CAST(value.string_value AS FLOAT64)) FROM UNNEST(user_properties) WHERE key = '${userPropertyName}') AS ${
        columnName ? columnName : userPropetyName
      }`;
      break;
    default:
      throw "userPropertyType is not valid";
  }
};



/**********************************************
上記で作成したモジュールをエクスポート
**********************************************/

module.exports = {
  getQueryString,
  deleteQueryString,
  setNumber,
  getFirstValue,
  getFirstValueNoColumnName,
  getLastValue,
  getLastValueNoColumnName,
  getPreviousPath,
  getNextPath,
  getEventParam,
  getEventParamNoColumnName,
  getEventParamAll,
  getUserProperty,
  getUserPropertyNoColumnName,
  getUserPropertyAll,
};