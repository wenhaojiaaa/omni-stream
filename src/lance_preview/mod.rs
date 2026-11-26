mod store;

use crate::{error::AppError, handlers::AppState, storage::StorageBackend};
use arrow_schema::DataType;
use axum::{
  extract::{Path, Query, State},
  http::header,
  response::{IntoResponse, Response},
};
use lance::{dataset::builder::DatasetBuilder, session::Session};
use lance_io::object_store::ObjectStoreRegistry;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
  error::Error as _,
  io::{self, Write},
  sync::Arc,
};
use tokio::sync::Semaphore;

static READERS: Semaphore = Semaphore::const_new(4);
const PAGE_ROWS: usize = 20;
const DISPLAY_BYTES: usize = 4 << 20;

// Arrow string escaping and vector formatting can expand a small binary
// batch considerably. Enforce the byte cap while constructing either JSON
// representation, including schema and omitted-value placeholders.
#[derive(Default)]
struct PageBuffer(Vec<u8>);

impl Write for PageBuffer {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    if buf.len() > DISPLAY_BYTES - self.0.len() {
      return Err(io::Error::new(
        io::ErrorKind::FileTooLarge,
        "Lance page exceeds the display budget",
      ));
    }
    self.0.extend_from_slice(buf);
    Ok(buf.len())
  }
  fn flush(&mut self) -> io::Result<()> {
    Ok(())
  }
}

#[derive(Serialize)]
struct DatasetPage {
  version: u64,
  total: usize,
  offset: usize,
  columns: Vec<Column>,
  rows: Vec<Value>,
}

#[derive(Serialize)]
struct Column {
  name: String,
  #[serde(rename = "type")]
  data_type: String,
  omitted: bool,
}

#[derive(Deserialize)]
pub struct PageQuery {
  storage: Option<String>,
  #[serde(default)]
  offset: usize,
  limit: Option<usize>,
  version: Option<u64>,
}

#[tracing::instrument(skip_all, fields(dataset = %key, offset = query.offset))]
pub async fn handler(
  State(state): State<AppState>,
  Path(key): Path<String>,
  Query(query): Query<PageQuery>,
) -> Result<Response, AppError> {
  let backend = state.resolve(query.storage.as_deref())?;
  let _permit = READERS
    .try_acquire()
    .map_err(|_| AppError::Busy("Lance previews are busy".into()))?;
  let page = page(backend, &key, query).await?;
  let mut output = PageBuffer::default();
  serde_json::to_writer(&mut output, &page).map_err(|e| AppError::Unsupported(e.to_string()))?;
  Ok(([(header::CONTENT_TYPE, "application/json")], output.0).into_response())
}

fn dataset_error(error: lance::Error) -> AppError {
  let mut cause = error.source();
  while let Some(err) = cause {
    if let Some(storage) = err.downcast_ref::<AppError>()
      && (matches!(
        storage,
        AppError::ObjectReadForbidden(_) | AppError::Forbidden(_)
      ) || matches!(storage, AppError::Io(e) if e.kind() == std::io::ErrorKind::PermissionDenied))
    {
      return AppError::Forbidden("access to the dataset was denied".into());
    }
    cause = err.source();
  }
  AppError::InvalidModel(format!("Lance: {error}"))
}

fn projected(ty: &DataType) -> bool {
  match ty {
    DataType::FixedSizeList(item, n) => (1..=2048).contains(n) && item.data_type().is_numeric(),
    DataType::Utf8
    | DataType::LargeUtf8
    | DataType::Utf8View
    | DataType::Boolean
    | DataType::Null => true,
    other => other.is_numeric() || other.is_temporal(),
  }
}

async fn page(
  backend: Arc<dyn StorageBackend>,
  root: &str,
  query: PageQuery,
) -> Result<DatasetPage, AppError> {
  let root = root.trim_end_matches('/');
  if !root.to_ascii_lowercase().ends_with(".lance")
    || root
      .split('/')
      .any(|s| matches!(s, "" | "." | "..") || s.contains('\\'))
  {
    return Err(AppError::InvalidPath(root.into()));
  }
  let limit = query.limit.unwrap_or(PAGE_ROWS);
  if !(1..=PAGE_ROWS).contains(&limit) {
    return Err(AppError::InvalidRange("limit must be 1..20".into()));
  }

  // A missing versions directory identifies an ordinary folder. Preserve all
  // subsequent errors (missing fragments, stale snapshots, corrupt manifests).
  if query.version.is_none() {
    backend
      .check_read_scope(root, &format!("{root}/_versions"))
      .await?;
    let versions = backend
      .list_files(&format!("{root}/_versions/"), None)
      .await?;
    if versions.entries.is_empty() {
      return Err(AppError::NotFound("Lance versions directory".into()));
    }
  }

  let registry = Arc::new(ObjectStoreRegistry::empty());
  let store = Arc::new(store::DatasetStore::new(backend, root));
  registry.insert("memory", store.clone());
  let session = Arc::new(Session::new(0, 2 << 20, registry));
  let mut builder = DatasetBuilder::from_uri("memory:///dataset").with_session(session);
  if let Some(version) = query.version {
    builder = builder.with_version(version);
  }
  let ds = builder.load().await.map_err(dataset_error)?;
  if !ds.manifest().base_paths.is_empty() {
    return Err(AppError::Unsupported("external Lance base paths".into()));
  }
  let total = ds.count_rows(None).await.map_err(dataset_error)?;
  if query.offset > total {
    return Err(AppError::InvalidRange(
      "offset exceeds dataset row count".into(),
    ));
  }
  let mut projection = ds.schema().clone();
  projection.fields.retain(|f| projected(&f.data_type()));
  let columns: Vec<_> = ds
    .schema()
    .fields
    .iter()
    .map(|field| {
      let ty = field.data_type();
      Column {
        name: field.name.clone(),
        data_type: ty.to_string(),
        omitted: !projected(&ty),
      }
    })
    .collect();
  let indices: Vec<_> = (query.offset..query.offset.saturating_add(limit).min(total))
    .map(|i| i as u64)
    .collect();
  let mut rows: Vec<Value> = if indices.is_empty() || projection.fields.is_empty() {
    vec![json!({}); indices.len()]
  } else {
    let batch = ds.take(&indices, projection).await.map_err(dataset_error)?;
    if batch.get_array_memory_size() > DISPLAY_BYTES {
      return Err(AppError::Unsupported(
        "Lance page exceeds the display budget".into(),
      ));
    }
    let mut writer = arrow_json::ArrayWriter::new(PageBuffer::default());
    writer
      .write(&batch)
      .and_then(|()| writer.finish())
      .map_err(|e| AppError::Unsupported(e.to_string()))?;
    serde_json::from_slice(&writer.into_inner().0).map_err(|e| AppError::Backend(e.to_string()))?
  };
  for row in &mut rows {
    for col in &columns {
      if col.omitted {
        row[&col.name] = json!("(value omitted)");
      } else if let Some(value) = row.get_mut(&col.name) {
        preserve_integer_precision(value);
      }
    }
  }
  tracing::debug!(
    range_bytes = store.bytes_requested(),
    offset = query.offset,
    "read Lance page"
  );
  Ok(DatasetPage {
    version: ds.version().version,
    total,
    offset: query.offset,
    columns,
    rows,
  })
}

// Preserve values that a JavaScript Number cannot represent exactly.
fn preserve_integer_precision(value: &mut Value) {
  const SAFE: u64 = (1 << 53) - 1;
  match value {
    Value::Number(n)
      if n.as_u64().is_some_and(|v| v > SAFE)
        || n.as_i64().is_some_and(|v| v.unsigned_abs() > SAFE) =>
    {
      *value = Value::String(n.to_string());
    }
    Value::Array(values) => values.iter_mut().for_each(preserve_integer_precision),
    _ => {}
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  #[tokio::test]
  async fn reads_last_page_through_storage_and_preserves_large_integers() {
    use crate::storage::local::LocalFsBackend;
    let dir = tempfile::tempdir().unwrap();
    let dataset = dir.path().join("fixture.lance");
    let schema =
      arrow_schema::Schema::new(vec![arrow_schema::Field::new("id", DataType::Int64, false)]);
    let reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
      .build(std::io::Cursor::new(
        b"{\"id\":1}\n{\"id\":9007199254740993}\n",
      ))
      .unwrap();
    lance::Dataset::write(reader, dataset.to_str().unwrap(), None)
      .await
      .unwrap();
    let storage = Arc::new(LocalFsBackend::new(dir.path(), false));
    let query = PageQuery {
      storage: None,
      offset: 1,
      limit: Some(20),
      version: Some(1),
    };
    let result = page(storage, "fixture.lance/", query).await.unwrap();
    assert_eq!(result.total, 2);
    assert_eq!(result.rows, vec![json!({"id":"9007199254740993"})]);
  }
  #[cfg(unix)]
  #[tokio::test]
  async fn members_must_resolve_inside_the_dataset() {
    use crate::storage::local::LocalFsBackend;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("preview.lance");
    tokio::fs::create_dir(&root).await.unwrap();
    tokio::fs::write(dir.path().join("sibling"), b"other dataset")
      .await
      .unwrap();
    tokio::fs::symlink("../sibling", root.join("part"))
      .await
      .unwrap();
    let backend = LocalFsBackend::new(dir.path(), true);
    assert!(matches!(
      backend
        .check_read_scope("preview.lance", "preview.lance/part")
        .await,
      Err(AppError::Forbidden(_))
    ));
  }

  #[test]
  fn excludes_unbounded_binary_and_large_vectors() {
    assert!(!projected(&DataType::Binary));
    let item = Arc::new(arrow_schema::Field::new("item", DataType::Float32, false));
    assert!(projected(&DataType::FixedSizeList(item.clone(), 768)));
    assert!(!projected(&DataType::FixedSizeList(item, 8192)));
  }
}
