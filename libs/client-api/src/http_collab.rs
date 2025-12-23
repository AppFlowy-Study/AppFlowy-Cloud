/* 
综合说明（中文）：

本文件实现了 Client 与后端协作（collab）相关的 HTTP 接口封装与辅助类型：
- 提供创建、更新、删除单个协作对象的接口（create_collab / update_collab / delete_collab）。
- 提供批量查询与批量创建接口（batch_post_collab / batch_get_collab / create_collab_list），
  包含对客户端侧并行压缩、帧化（frame）以及分块上传的实现细节。
- 实现了实时消息的发送（post_realtime_msg）与发布流式协作项（publish_collabs），
  使用了流（Stream）抽象以支持分块/流式传输并提供对应的序列化/帧协议。
- 支持与数据库行相关的一系列操作：列出数据库、获取字段、添加/upsert 行、获取行详情与更新检测等。
- 对于需要传输二进制或 protobuf 的接口（如 collab_full_sync），实现了对数据的压缩（zstd / brotli）、编码与解码流程，
  并在必要时对响应体进行解压与反序列化。
- 包含重试策略支持（基于 tokio_retry），对可重试错误进行了封装与判定（RetryGetCollabCondition / GetCollabAction）。
- 广泛使用了项目内的错误类型（AppError / AppResponseError）、追踪/日志（tracing::instrument / event）以及
  reqwest 的异步 HTTP 客户端构建器（含鉴权与压缩扩展方法）。

注意事项与约定：
- 本文件中对“帧头/长度字段”的字节序（big-endian / little-endian）在注释处有说明，客户端与服务端必须保持一致。
- 元数据与数据打包规则在 serialize_metadata_data 中定义（长度字段 + 元数据 + 数据），用于 publish 流。
- 本文件的各个异步方法通常返回 Result<T, AppResponseError>，并通过 process_response_* 系列助手函数
  统一处理 HTTP 响应与错误映射。

该注释旨在为阅读者提供文件功能的整体视图，具体实现细节请参考各函数的逐行注释。
*/
use crate::entity::CollabType; // 导入当前 crate（包）中 entity 模块下的 CollabType 类型（用于表示协作对象的类型）
use crate::{
  blocking_brotli_compress, brotli_compress, process_response_data, process_response_error, Client,
}; // 从当前 crate 引入多个函数/类型：同步压缩、异步压缩、处理响应数据/错误的助手函数，以及 Client 结构体
use anyhow::anyhow; // 引入 anyhow 的 anyhow! 宏/函数，用于创建通用错误（便于包装不同类型错误）
use app_error::AppError; // 引入项目自定义的错误类型 AppError，用于将底层错误映射到应用层错误
use bytes::Bytes; // 引入 bytes::Bytes，常用于高效的字节缓冲与传输
use chrono::{DateTime, Utc}; // 引入 chrono 的 DateTime 与 Utc（UTC 时区），用于时间/日期字段
use client_api_entity::workspace_dto::{
  AFDatabase, AFDatabaseField, AFDatabaseRow, AFDatabaseRowDetail, AFInsertDatabaseField,
  AddDatatabaseRow, DatabaseRowUpdatedItem, ListDatabaseRowDetailParam,
  ListDatabaseRowUpdatedParam, UpsertDatatabaseRow,
}; // 引入一组与工作区数据库相关的 DTO（数据传输对象）结构体（注意：多行写法是为了可读性）
use client_api_entity::{
  AFCollabEmbedInfo, AFDatabaseRowDocumentCollabExistenceInfo, BatchQueryCollabParams,
  BatchQueryCollabResult, CollabParams, CreateCollabData, CreateCollabParams, DeleteCollabParams,
  PublishCollabItem, QueryCollab, QueryCollabParams, RepeatedAFCollabEmbedInfo,
  UpdateCollabWebParams,
}; // 从另一个模块引入更多与协作（collab）相关的 DTO/参数/类型
use collab_rt_entity::collab_proto::{CollabDocStateParams, PayloadCompressionType}; // 引入 protobuf 生成的参数类型与压缩类型枚举
use collab_rt_entity::HttpRealtimeMessage; // 引入用于实时消息的结构体
use futures::Stream; // 引入 futures::Stream trait，用于实现异步流（stream）
use futures_util::stream; // 引入 futures_util::stream 模块（提供辅助函数，比如 stream::iter）
use prost::Message; // 引入 prost::Message trait，用于 protobuf 消息的 encode/decode
use rayon::prelude::*; // 引入 rayon 的并行迭代工具（prelude::*），用于并发地处理集合
use reqwest::{Body, Method}; // 引入 reqwest 的 Body 和 Method（用于 HTTP 请求体与方法）
use serde::Serialize; // 引入 serde 的 Serialize trait，用于序列化泛型元数据
use shared_entity::dto::workspace_dto::{CollabResponse, CollabTypeParam, EmbeddedCollabQuery}; // 引入共享实体层的 DTO 类型
use shared_entity::response::AppResponseError; // 引入共享的响应错误类型，用作方法返回错误
use std::collections::HashMap; // 引入标准库的 HashMap（键值映射）
use std::future::Future; // 引入 Future trait（在后面手动实现异步动作时会用到）
use std::io::Cursor; // 引入 Cursor，用于将 Vec<u8> 包装为实现 Read 的对象（常用于压缩/解压）
use std::pin::Pin; // 引入 Pin，用于不可移动类型的引用（在实现异步 trait 时常见）
use std::task::{Context, Poll}; // 引入任务上下文 Context 与 Poll（实现 Stream 或 Future 的核心 API）
use std::time::Duration; // 引入 Duration（时间段），用于设置超时等
use tokio_retry::strategy::ExponentialBackoff; // 引入 tokio_retry 的指数回退策略
use tokio_retry::{Action, Condition, RetryIf}; // 引入重试相关的 trait/函数（Action、Condition、RetryIf）
use tracing::{event, instrument}; // 引入 tracing 的 event 和 instrument 宏，用于日志与追踪
use uuid::Uuid; // 引入 uuid::Uuid 类型（表示唯一标识符）

impl Client { // 为 Client 类型实现一组方法（这些方法封装了与 server 的 HTTP 调用）
  #[instrument(level = "info", skip_all, err)] // tracing 的 attribute：在调用时记录追踪信息；skip_all 表示跳过记录所有参数值（降低日志噪音），err 表示在发生错误时记录
  pub async fn create_collab(&self, params: CreateCollabParams) -> Result<(), AppResponseError> { // 异步公有方法，接受引用自身与 CreateCollabParams，返回可能的 AppResponseError
    let url = format!( // 使用 format! 构造最终的 URL 字符串
      "{}/api/workspace/{}/collab/{}", // 模板字符串：第一个 {} 是 base_url，第二个是 workspace_id，第三个是 object_id
      self.base_url, params.workspace_id, &params.object_id
    );
    let bytes = params
      .to_bytes() // 假设 CreateCollabParams 上实现了 to_bytes（返回 Vec<u8> 或 Result）
      .map_err(|err| AppError::Internal(err.into()))?; // 若失败，将底层错误转换为 AppError::Internal 并返回（? 会早期返回错误）

    let compress_bytes = blocking_brotli_compress( // 调用同步/阻塞式的 brotli 压缩包装（该函数返回一个 Future，因此后面有 .await）
      bytes,
      self.config.compression_quality, // 压缩质量参数（配置）
      self.config.compression_buffer_size, // 压缩缓冲区大小（配置）
    )
    .await?; // await 返回 Result，? 用于传播错误（如果发生）

    #[allow(unused_mut)] // 屏蔽未使用可变变量的编译警告（允许 builder 在某些 target 下可能不被修改）
    let mut builder = self
      .http_client_with_auth_compress(Method::POST, &url) // 构造带鉴权与压缩支持的 reqwest 客户端构建器（假设 Client 有此方法）
      .await?; // await 并传播可能的错误

    #[cfg(not(target_arch = "wasm32"))] // 条件编译：如果不是 wasm32 目标（即非浏览器环境）则编译下面的代码块
    {
      builder = builder.timeout(std::time::Duration::from_secs(60)); // 在非 wasm 环境下为请求设置 60 秒超时
    }

    let resp = builder.body(compress_bytes).send().await?; // 将压缩后的字节设置为请求体并发送，await 后获取响应或错误
    process_response_error(resp).await // 处理响应错误：该函数会检查 HTTP 状态并将错误转换为 AppResponseError
  }

  #[instrument(level = "info", skip_all, err)]
  pub async fn update_collab(&self, params: CreateCollabParams) -> Result<(), AppResponseError> { // 更新协作对象（PUT）
    let url = format!(
      "{}/api/workspace/{}/collab/{}",
      self.base_url, &params.workspace_id, &params.object_id
    );
    let resp = self
      .http_client_with_auth(Method::PUT, &url) // 构造带鉴权的 PUT 请求构建器
      .await?
      .json(&params) // 将 params 序列化为 JSON 作为请求体（reqwest 的 .json 会自动设置 Content-Type）
      .send()
      .await?;
    process_response_error(resp).await // 处理响应并返回结果
  }

  pub async fn update_web_collab( // 面向 web 的更新接口（POST）
    &self,
    workspace_id: &Uuid, // 引用类型：避免克隆 Uuid（节省开销）
    object_id: &Uuid,
    params: UpdateCollabWebParams,
  ) -> Result<(), AppResponseError> {
    let url = format!(
      "{}/api/workspace/v1/{}/collab/{}/web-update",
      self.base_url, workspace_id, object_id
    );
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    process_response_error(resp).await
  }

  // The browser will call this API to get the collab list, because the URL length limit and browser can't send the body in GET request
  #[instrument(level = "info", skip_all, err)]
  pub async fn batch_post_collab( // 批量查询协作项（POST 方式，适配浏览器 URL 长度限制）
    &self,
    workspace_id: &Uuid,
    params: Vec<QueryCollab>,
  ) -> Result<BatchQueryCollabResult, AppResponseError> {
    self
      .send_batch_collab_request(Method::POST, workspace_id, params) // 复用内部实现函数，传入 POST
      .await
  }

  #[instrument(level = "info", skip_all, err)]
  pub async fn batch_get_collab( // 批量查询协作项（GET 方式）
    &self,
    workspace_id: &Uuid,
    params: Vec<QueryCollab>,
  ) -> Result<BatchQueryCollabResult, AppResponseError> {
    self
      .send_batch_collab_request(Method::GET, workspace_id, params) // 复用内部实现函数，传入 GET
      .await
  }

  async fn send_batch_collab_request( // 发送批量协作查询请求的内部函数（被上面的两个方法复用）
    &self,
    method: Method, // reqwest::Method（GET/POST 等）
    workspace_id: &Uuid,
    params: Vec<QueryCollab>,
  ) -> Result<BatchQueryCollabResult, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/collab_list",
      self.base_url, workspace_id
    );
    let params = BatchQueryCollabParams(params); // 包装参数为服务器期望的结构
    let resp = self
      .http_client_with_auth(method, &url) // 根据传入 method 构造请求
      .await?
      .json(&params)
      .send()
      .await?;
    process_response_data::<BatchQueryCollabResult>(resp).await // 反序列化并返回数据类型
  }

  #[instrument(level = "info", skip_all, err)]
  pub async fn delete_collab(&self, params: DeleteCollabParams) -> Result<(), AppResponseError> { // 删除协作对象
    let url = format!(
      "{}/api/workspace/{}/collab/{}",
      self.base_url, &params.workspace_id, &params.object_id
    );
    let resp = self
      .http_client_with_auth(Method::DELETE, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    process_response_error(resp).await
  }

  #[instrument(level = "info", skip_all, err)]
  pub async fn list_databases(
    &self,
    workspace_id: &Uuid,
  ) -> Result<Vec<AFDatabase>, AppResponseError> { // 列出工作区下的数据库
    let url = format!("{}/api/workspace/{}/database", self.base_url, workspace_id);
    let resp = self
      .http_client_with_auth(Method::GET, &url)
      .await?
      .send()
      .await?;
    process_response_data::<Vec<AFDatabase>>(resp).await
  }

  pub async fn list_database_row_ids(
    &self,
    workspace_id: &Uuid,
    database_id: &str,
  ) -> Result<Vec<AFDatabaseRow>, AppResponseError> { // 列出指定数据库的行 id 列表
    let url = format!(
      "{}/api/workspace/{}/database/{}/row",
      self.base_url, workspace_id, database_id
    );
    let resp = self
      .http_client_with_auth(Method::GET, &url)
      .await?
      .send()
      .await?;
    process_response_data::<Vec<AFDatabaseRow>>(resp).await
  }

  pub async fn get_database_fields( // 获取数据库字段定义：构造 GET 请求并返回字段列表
    &self, // 方法接收者：对 Client 的不可变借用
    workspace_id: &Uuid, // 工作区 ID 的引用，使用引用以避免复制 Uuid 值
    database_id: &str, // 数据库 ID 字符串切片引用（借用，不拥有）
  ) -> Result<Vec<AFDatabaseField>, AppResponseError> { // 返回结果为 AFDatabaseField 向量或 AppResponseError
    let url = format!( // 使用 format! 构造请求 URL
      "{}/api/workspace/{}/database/{}/fields", // 模板：base_url / workspace / database / fields
      self.base_url, workspace_id, database_id
    );
    let resp = self
      .http_client_with_auth(Method::GET, &url) // 创建带鉴权的 GET 请求构建器
      .await? // await 表示异步等待；? 操作符将错误向上传播为函数的返回值
      .send() // 发送请求
      .await?; // 等待响应并传播错误
    process_response_data::<Vec<AFDatabaseField>>(resp).await // 处理响应并尝试解码为 Vec<AFDatabaseField>
  }
  // Adds a database field to the specified database.
  // Returns the field id of the newly created field.
  pub async fn add_database_field( // 添加新的数据库字段（POST）
    &self,
    workspace_id: &Uuid,
    database_id: &str,
    insert_field: &AFInsertDatabaseField, // 引用传入的插入字段对象，避免克隆
  ) -> Result<String, AppResponseError> { // 返回新字段的 id（字符串）或错误
    let url = format!( // 构造 POST 的 endpoint URL
      "{}/api/workspace/{}/database/{}/fields",
      self.base_url, workspace_id, database_id
    );
    let resp = self
      .http_client_with_auth(Method::POST, &url) // 使用 POST 方法
      .await?
      .json(insert_field) // 将 insert_field 序列化为 JSON 作为请求体（reqwest 自动设置 Content-Type）
      .send()
      .await?; // 发送并 await
    process_response_data::<String>(resp).await // 将响应解码为 String（字段 id）并返回
  }

  pub async fn list_database_row_ids_updated( // 列出自某时间点之后更新过的行 id 列表
    &self,
    workspace_id: &Uuid,
    database_id: &str,
    after: Option<DateTime<Utc>>, // 可选参数，表示筛选更新时间在该时间之后的行
  ) -> Result<Vec<DatabaseRowUpdatedItem>, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/database/{}/row/updated", // 指向 row/updated 的 endpoint
      self.base_url, workspace_id, database_id
    );
    let resp = self
      .http_client_with_auth(Method::GET, &url) // GET 请求
      .await?
      .query(&ListDatabaseRowUpdatedParam { after }) // 使用 .query 方法添加查询参数（会序列化为 ?after=...）
      .send()
      .await?;
    process_response_data::<Vec<DatabaseRowUpdatedItem>>(resp).await // 解析为 DatabaseRowUpdatedItem 列表
  }

  pub async fn list_database_row_details( // 获取多行的详细信息（可选择是否包含文档）
    &self,
    workspace_id: &Uuid,
    database_id: &str,
    row_ids: &[&str], // 接收一个字符串切片数组的引用（借用不存在所有权）
    with_doc: bool, // 是否包含文档内容的布尔标志
  ) -> Result<Vec<AFDatabaseRowDetail>, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/database/{}/row/detail", // endpoint：row/detail
      self.base_url, workspace_id, database_id
    );
    let resp = self
      .http_client_with_auth(Method::GET, &url)
      .await?
      .query(&ListDatabaseRowDetailParam::new(row_ids, with_doc)) // 使用构造函数生成查询参数结构体
      .send()
      .await?;
    process_response_data::<Vec<AFDatabaseRowDetail>>(resp).await // 返回解析后的详细行信息向量
  }

  /// Example payload:
  /// {
  ///   "Name": "some_data",        # using column name
  ///   "_pIkG": "some other data"  # using field_id (can be obtained from [get_database_fields])
  /// }
  /// Upon success, returns the row id for the newly created row.
  pub async fn add_database_item( // 在指定数据库中添加一条数据行
    &self,
    workspace_id: &Uuid,
    database_id: &str,
    cells_by_id: HashMap<String, serde_json::Value>, // 使用 HashMap 表示列 id -> 值（JSON 值）
    row_doc_content: Option<String>, // 可选的文档内容（例如富文本或文档序列化）
  ) -> Result<String, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/database/{}/row", // POST 到 /row 用于创建新行
      self.base_url, workspace_id, database_id
    );
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .json(&AddDatatabaseRow { // 构造请求体：AddDatatabaseRow 结构体字面量
        cells: cells_by_id, // 将传入的 cells_by_id 放入结构体
        document: row_doc_content, // 可选文档字段
      })
      .send()
      .await?;
    process_response_data::<String>(resp).await // 成功则返回新建行的 id（String）
  }

  /// Like [add_database_item], but use a [pre_hash] as identifier of the row
  /// Given the same `pre_hash` value will result in the same row
  /// Creates the row if now exists, else row will be modified
  pub async fn upsert_database_item( // upsert：插入或更新（如果存在则更新）
    &self,
    workspace_id: &Uuid,
    database_id: &str,
    pre_hash: String, // 用作行唯一标识的预哈希值（传入所有权）
    cells_by_id: HashMap<String, serde_json::Value>,
    row_doc_content: Option<String>,
  ) -> Result<String, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/database/{}/row", // PUT 到相同的 /row endpoint 表示 upsert 行为
      self.base_url, workspace_id, database_id
    );
    let resp = self
      .http_client_with_auth(Method::PUT, &url) // 使用 PUT 方法（语义上表示替换/更新）
      .await?
      .json(&UpsertDatatabaseRow { // 构造 UpsertDatatabaseRow 请求体
        pre_hash, // 将 pre_hash 移动到请求体（传入所有权）
        cells: cells_by_id,
        document: row_doc_content,
      })
      .send()
      .await?;
    process_response_data::<String>(resp).await // 返回行 id（如果创建或更新成功）
  }

  #[instrument(level = "debug", skip_all, err)]
  pub async fn post_realtime_msg( // 向服务器推送实时消息（HTTP 流）
    &self,
    device_id: &str, // 设备 id 的字符串切片引用
    msg: client_websocket::Message, // 消息对象（按值传入，取决于 Message 的大小与实现）
  ) -> Result<(), AppResponseError> {
    let device_id = device_id.to_string(); // 将 &str 转为 String（克隆数据）
    let payload =
      blocking_brotli_compress(msg.into_data(), 6, self.config.compression_buffer_size).await?; // 对消息数据进行 brotli 压缩（quality=6）

    let msg = HttpRealtimeMessage { device_id, payload }.encode_to_vec(); // 使用 protobuf Message.encode_to_vec() 获取序列化字节
    let body = Body::wrap_stream(stream::iter(vec![Ok::<_, reqwest::Error>(msg)])); // 把消息包装为异步流（reqwest Body 支持）
    let url = format!("{}/api/realtime/post/stream", self.base_url); // 实时推送 endpoint
    let resp = self
      .http_client_with_auth_compress(Method::POST, &url) // 使用支持压缩的鉴权客户端构造请求
      .await?
      .body(body) // 将流体作为请求体
      .send()
      .await?;
    process_response_error(resp).await // 处理响应错误，返回 Result<(), AppResponseError>
  }

  #[instrument(level = "debug", skip_all, err)]
  pub async fn create_collab_list( // 批量创建协作对象：并发压缩并发送框架化数据
    &self,
    workspace_id: &Uuid,
    params_list: Vec<CollabParams>, // 接收一组协作参数（按值移动）
  ) -> Result<(), AppResponseError> {
    let url = self.batch_create_collab_url(workspace_id); // 获取批量创建的 URL（假设 Client 提供该方法）
 
    let compression_tasks = params_list
      .into_par_iter() // rayon 的并行迭代器：将向量转为可并行处理的迭代器
      .filter_map(|params| {
        let data = CreateCollabData::from(params).to_bytes().ok()?; // 将 CollabParams 转为 CreateCollabData 并尝试获取字节
        brotli_compress(
          data,
          self.config.compression_quality,
          self.config.compression_buffer_size,
        )
        .ok() // brotli_compress 返回 Result，这里将其映射为 Option（失败则过滤）
      })
      .collect::<Vec<_>>(); // 收集所有成功压缩的字节为向量

    let mut framed_data = Vec::new(); // 用于存放带帧头的压缩数据
    let mut size_count = 0; // 统计总大小（用于日志）
    for compressed in compression_tasks {
      // The length of a u32 in bytes is 4. The server uses a u32 to read the size of each data frame,
      // hence the frame size header is always 4 bytes. It's crucial not to alter this size value,
      // as the server's logic for frame size reading is based on this fixed 4-byte length.
      // note:
      // the size of a u32 is a constant 4 bytes across all platforms that Rust supports.
      let size = compressed.len() as u32; // 计算压缩块长度并转换为 u32（网络序或指定字节序）
      framed_data.extend_from_slice(&size.to_be_bytes()); // 将长度的 big-endian 字节添加到帧头（服务器期望 big-endian）
      framed_data.extend_from_slice(&compressed); // 添加实际压缩数据
      size_count += size; // 累加大小统计
    }
    event!(
      tracing::Level::INFO,
      "create batch collab with size: {}", // 记录创建批量协作的总大小到日志
      size_count
    );
    let body = Body::wrap_stream(stream::once(async { Ok::<_, AppError>(framed_data) })); // 将 framed_data 包装为单次异步流
    let resp = self
      .http_client_with_auth_compress(Method::POST, &url)
      .await?
      .timeout(Duration::from_secs(60)) // 为批量请求设置超时（60 秒）
      .body(body)
      .send()
      .await?;

    process_response_error(resp).await // 处理响应错误
  }
  /// Example payload:
  /// {
  ///   "Name": "some_data",        # using column name
  ///   "_pIkG": "some other data"  # using field_id (can be obtained from [get_database_fields])
  /// }
  /// Upon success, returns the row id for the newly created row.
  pub async fn add_database_item(
    &self,
    workspace_id: &Uuid,
    database_id: &str,
    cells_by_id: HashMap<String, serde_json::Value>,
    row_doc_content: Option<String>,
  ) -> Result<String, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/database/{}/row",
      self.base_url, workspace_id, database_id
    );
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .json(&AddDatatabaseRow {
        cells: cells_by_id,
        document: row_doc_content,
      })
      .send()
      .await?;
    process_response_data::<String>(resp).await
  }

  /// Like [add_database_item], but use a [pre_hash] as identifier of the row
  /// Given the same `pre_hash` value will result in the same row
  /// Creates the row if now exists, else row will be modified
  pub async fn upsert_database_item(
    &self,
    workspace_id: &Uuid,
    database_id: &str,
    pre_hash: String,
    cells_by_id: HashMap<String, serde_json::Value>,
    row_doc_content: Option<String>,
  ) -> Result<String, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/database/{}/row",
      self.base_url, workspace_id, database_id
    );
    let resp = self
      .http_client_with_auth(Method::PUT, &url)
      .await?
      .json(&UpsertDatatabaseRow {
        pre_hash,
        cells: cells_by_id,
        document: row_doc_content,
      })
      .send()
      .await?;
    process_response_data::<String>(resp).await
  }

  #[instrument(level = "debug", skip_all, err)]
  pub async fn post_realtime_msg(
    &self,
    device_id: &str,
    msg: client_websocket::Message,
  ) -> Result<(), AppResponseError> {
    let device_id = device_id.to_string();
    let payload =
      blocking_brotli_compress(msg.into_data(), 6, self.config.compression_buffer_size).await?;

    let msg = HttpRealtimeMessage { device_id, payload }.encode_to_vec();
    let body = Body::wrap_stream(stream::iter(vec![Ok::<_, reqwest::Error>(msg)]));
    let url = format!("{}/api/realtime/post/stream", self.base_url);
    let resp = self
      .http_client_with_auth_compress(Method::POST, &url)
      .await?
      .body(body)
      .send()
      .await?;
    process_response_error(resp).await
  }

  #[instrument(level = "debug", skip_all, err)]
  pub async fn create_collab_list(
    &self,
    workspace_id: &Uuid,
    params_list: Vec<CollabParams>,
  ) -> Result<(), AppResponseError> {
    let url = self.batch_create_collab_url(workspace_id);

    let compression_tasks = params_list
      .into_par_iter()
      .filter_map(|params| {
        let data = CreateCollabData::from(params).to_bytes().ok()?;
        brotli_compress(
          data,
          self.config.compression_quality,
          self.config.compression_buffer_size,
        )
        .ok()
      })
      .collect::<Vec<_>>();

    let mut framed_data = Vec::new();
    let mut size_count = 0;
    for compressed in compression_tasks {
      // The length of a u32 in bytes is 4. The server uses a u32 to read the size of each data frame,
      // hence the frame size header is always 4 bytes. It's crucial not to alter this size value,
      // as the server's logic for frame size reading is based on this fixed 4-byte length.
      // note:
      // the size of a u32 is a constant 4 bytes across all platforms that Rust supports.
      let size = compressed.len() as u32;
      framed_data.extend_from_slice(&size.to_be_bytes());
      framed_data.extend_from_slice(&compressed);
      size_count += size;
    }
    event!(
      tracing::Level::INFO,
      "create batch collab with size: {}",
      size_count
    );
    let body = Body::wrap_stream(stream::once(async { Ok::<_, AppError>(framed_data) }));
    let resp = self
      .http_client_with_auth_compress(Method::POST, &url)
      .await?
      .timeout(Duration::from_secs(60))
      .body(body)
      .send()
      .await?;

    process_response_error(resp).await
  }

  #[instrument(level = "debug", skip_all)]
  pub async fn get_collab(
    &self,
    params: QueryCollabParams,
  ) -> Result<CollabResponse, AppResponseError> {
    // 2 seconds, 4 seconds, 8 seconds
    let retry_strategy = ExponentialBackoff::from_millis(2).factor(1000).take(3);
    let action = GetCollabAction::new(self.clone(), params);
    RetryIf::spawn(retry_strategy, action, RetryGetCollabCondition).await
  }

  pub async fn publish_collabs<Metadata, Data>(
    &self,
    workspace_id: &Uuid,
    items: Vec<PublishCollabItem<Metadata, Data>>,
  ) -> Result<(), AppResponseError>
  where
    Metadata: serde::Serialize + Send + 'static + Unpin,
    Data: AsRef<[u8]> + Send + 'static + Unpin,
  {
    let publish_collab_stream = PublishCollabItemStream::new(items);
    let url = format!("{}/api/workspace/{}/publish", self.base_url, workspace_id,);
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .body(Body::wrap_stream(publish_collab_stream))
      .send()
      .await?;
    process_response_error(resp).await
  }

  pub async fn check_if_row_document_collab_exists(
    &self,
    workspace_id: &Uuid,
    object_id: &Uuid,
  ) -> Result<bool, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{workspace_id}/collab/{object_id}/row-document-collab-exists",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::GET, &url)
      .await?
      .send()
      .await?;
    let info = process_response_data::<AFDatabaseRowDocumentCollabExistenceInfo>(resp).await?;
    Ok(info.exists)
  }

  pub async fn get_collab_embed_info(
    &self,
    workspace_id: &Uuid,
    object_id: &Uuid,
  ) -> Result<AFCollabEmbedInfo, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{workspace_id}/collab/{object_id}/embed-info",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::GET, &url)
      .await?
      .header("Content-Type", "application/json")
      .send()
      .await?;
    process_response_data::<AFCollabEmbedInfo>(resp).await
  }

  pub async fn batch_get_collab_embed_info(
    &self,
    workspace_id: &Uuid,
    params: Vec<EmbeddedCollabQuery>,
  ) -> Result<Vec<AFCollabEmbedInfo>, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{workspace_id}/collab/embed-info/list",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    process_response_data::<RepeatedAFCollabEmbedInfo>(resp)
      .await
      .map(|data| data.0)
  }

  pub async fn force_generate_collab_embeddings(
    &self,
    workspace_id: &Uuid,
    object_id: &Uuid,
  ) -> Result<(), AppResponseError> {
    let url = format!(
      "{}/api/workspace/{workspace_id}/collab/{object_id}/generate-embedding",
      self.base_url
    );
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .send()
      .await?;
    process_response_error(resp).await
  }

  pub async fn collab_full_sync(
    &self,
    workspace_id: &Uuid,
    object_id: &Uuid,
    collab_type: CollabType,
    doc_state: Vec<u8>,
    state_vector: Vec<u8>,
  ) -> Result<Vec<u8>, AppResponseError> {
    let url = format!(
      "{}/api/workspace/v1/{workspace_id}/collab/{object_id}/full-sync",
      self.base_url
    );

    // 3 is default level
    let doc_state = zstd::encode_all(Cursor::new(doc_state), 3)
      .map_err(|err| AppError::InvalidRequest(format!("Failed to compress text: {}", err)))?;

    let sv = zstd::encode_all(Cursor::new(state_vector), 3)
      .map_err(|err| AppError::InvalidRequest(format!("Failed to compress text: {}", err)))?;

    let params = CollabDocStateParams {
      object_id: object_id.to_string(),
      collab_type: collab_type.value(),
      compression: PayloadCompressionType::Zstd as i32,
      sv,
      doc_state,
    };

    let mut encoded_payload = Vec::new();
    params.encode(&mut encoded_payload).map_err(|err| {
      AppError::Internal(anyhow!("Failed to encode CollabDocStateParams: {}", err))
    })?;

    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .body(Bytes::from(encoded_payload))
      .send()
      .await?;
    if resp.status().is_success() {
      let body = resp.bytes().await?;
      let decompressed_body = zstd::decode_all(Cursor::new(body))?;
      Ok(decompressed_body)
    } else {
      process_response_data::<Vec<u8>>(resp).await
    }
  }
}

struct RetryGetCollabCondition; // 空结构体：作为重试判定条件的类型占位
impl Condition<AppResponseError> for RetryGetCollabCondition { // 为重试判定实现 Condition trait
  fn should_retry(&mut self, error: &AppResponseError) -> bool { // 判断是否应当重试
    !error.is_record_not_found() // 如果错误表示“记录未找到”，则不重试；否则允许重试
  }
}

pub struct PublishCollabItemStream<Metadata, Data> { // 定义流式发布协作项的结构体
  items: Vec<PublishCollabItem<Metadata, Data>>, // 存放要发布的项（拥有所有权）
  idx: usize, // 当前项的索引
  done: bool, // 是否已发送结束标记
}

impl<Metadata, Data> PublishCollabItemStream<Metadata, Data> {
  pub fn new(publish_collab_items: Vec<PublishCollabItem<Metadata, Data>>) -> Self { // 构造函数：接受一个向量并返回实例
    PublishCollabItemStream {
      items: publish_collab_items, // 移动传入向量所有权
      idx: 0, // 初始化索引
      done: false, // 初始化未完成状态
    }
  }
}

impl<Metadata, Data> Stream for PublishCollabItemStream<Metadata, Data> // 为结构体实现 futures::Stream trait
where
  Metadata: Serialize + Send + 'static + Unpin, // 元数据的 trait 约束：可序列化、可在线程间发送、静态生命周期且 Unpin
  Data: AsRef<[u8]> + Send + 'static + Unpin, // 数据约束：可按字节切片访问、可发送、静态且 Unpin
{
  type Item = Result<Bytes, std::io::Error>; // 流的每个项是 Result<Bytes, io::Error>

  fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> { // 推进流的方法
    let mut self_mut = self.as_mut(); // 将 Pin<&mut Self> 转为可变引用以访问字段

    if self_mut.idx >= self_mut.items.len() { // 若索引超出范围，表示所有项已处理
      if !self_mut.done { // 如果还未发送结束标记
        self_mut.done = true; // 标记为已发送结束标记
        return Poll::Ready(Some(Ok((0_u32).to_le_bytes().to_vec().into()))); // 返回一个特殊的结束帧（小端 u32 0 表示）
      }
      return Poll::Ready(None); // 已经发送结束帧，流结束
    }

    let item = &self_mut.items[self_mut.idx]; // 取当前项的引用
    match serialize_metadata_data(&item.meta, item.data.as_ref()) { // 将元数据与数据合并为一个 chunk
      Err(e) => Poll::Ready(Some(Err(e))), // 若序列化失败，返回错误
      Ok(chunk) => {
        self_mut.idx += 1; // 增加索引以便下次处理下一项
        Poll::Ready(Some(Ok::<bytes::Bytes, std::io::Error>(chunk))) // 返回序列化后的字节
      },
    }
  }
}

fn serialize_metadata_data<Metadata>(m: Metadata, d: &[u8]) -> Result<Bytes, std::io::Error> // 将元数据与数据拼装成二进制块
where
  Metadata: Serialize, // 元数据必须实现 Serialize trait
{
  let meta = serde_json::to_vec(&m)?; // 把元数据序列化为 JSON 字节向量（可能返回错误）

  let mut chunk = Vec::with_capacity(8 + meta.len() + d.len()); // 预分配容量：两个 u32 长度字段 + 元数据 + 数据
  chunk.extend_from_slice(&(meta.len() as u32).to_le_bytes()); // 写入元数据长度（u32，小端）
  chunk.extend_from_slice(&meta); // 写入元数据
  chunk.extend_from_slice(&(d.len() as u32).to_le_bytes()); // 写入数据长度（u32，小端）
  chunk.extend_from_slice(d); // 写入数据本体

  Ok(Bytes::from(chunk)) // 返回 bytes::Bytes 包装的字节块
}

pub(crate) struct GetCollabAction { // 定义一个用于重试机制的 Action，封装 client 与 params
  client: Client, // 持有 Client 的实例（通常是克隆）
  params: QueryCollabParams, // 请求参数（拥有所有权）
}

impl GetCollabAction {
  pub fn new(client: Client, params: QueryCollabParams) -> Self { // 构造函数：接受 client 和参数并返回实例
    Self { client, params }
  }
}

impl Action for GetCollabAction { // 为 GetCollabAction 实现 tokio_retry::Action trait
  type Future = Pin<Box<dyn Future<Output = Result<Self::Item, Self::Error>> + Send + Sync>>; // 复杂的 Future 类型声明（trait 对象包装）
  type Item = CollabResponse; // 成功返回类型
  type Error = AppResponseError; // 错误类型

  fn run(&mut self) -> Self::Future { // run 方法会在重试策略中被调用以执行请求
    let client = self.client.clone(); // 克隆 client，因为要把它移动进异步闭包中
    let params = self.params.clone(); // 克隆参数，保证异步闭包拥有所有权
    let collab_type = self.params.collab_type; // 读取 collab_type（按需复制或拷贝）

    Box::pin(async move { // 返回一个 Pin<Box<dyn Future>>
      let url = format!(
        "{}/api/workspace/v1/{}/collab/{}",
        client.base_url, &params.workspace_id, &params.object_id
      ); // 构造请求 URL
      let resp = client
        .http_client_with_auth(Method::GET, &url)
        .await? // 获取请求构建器并等待
        .query(&CollabTypeParam { collab_type }) // 添加查询参数
        .send()
        .await?; // 发送请求并等待响应
      process_response_data::<CollabResponse>(resp).await // 解析响应为 CollabResponse 并返回
    })
  }
}
