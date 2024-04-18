// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use aws_types::sdk_config::SdkConfig;
use bytesize::ByteSize;
use mz_arrow_util::builder::ArrowBuilder;
use mz_aws_util::s3_uploader::{
    CompletedUpload, S3MultiPartUploader, S3MultiPartUploaderConfig, AWS_S3_MAX_PART_COUNT,
};
use mz_ore::cast::CastFrom;
use mz_ore::future::OreFutureExt;
use mz_repr::{GlobalId, RelationDesc, Row};
use mz_storage_types::sinks::{S3SinkFormat, S3UploadInfo};
use parquet::arrow::arrow_writer::ArrowWriter;

use super::{CopyToS3Uploader, S3KeyManager};

/// The max buffer size for the ArrowBuilder before we flush its contents to to the
/// ParquetFile writer, which may flush to the S3 uploader if the row group size
/// exceeds the limit (see `PARQUET_ROW_GROUP_SIZE`).
/// NOTE that this ends up being per-batch since there is an independent ParquetUploader
/// per batch.
const DEFAULT_ARROW_BUILDER_BUFFER_SIZE: ByteSize = ByteSize::mib(32);

/// The max size we allow each 'row group' in the parquet files to be. Each time the writer
/// writes a row group, we provide its contents to the S3 uploader. We want to keep
/// these row groups small enough to upload regularly but large enough that they don't
/// make the parquet files inefficient to read.
const PARQUET_ROW_GROUP_SIZE: ByteSize = ByteSize::mib(32);

/// The size of each part in the multi-part upload to use when uploading parquet files to S3.
const UPLOAD_MULTIPART_PART_SIZE_LIMIT: ByteSize = ByteSize::mib(8);

/// Set the default capacity for the array builders inside the ArrowBuilder. This is the
/// number of items each builder can hold before it needs to allocate more memory.
const DEFAULT_ARRAY_BUILDER_ITEM_CAPACITY: usize = 1024;
/// Set the default buffer size for the string and binary array builders inside the ArrowBuilder.
/// This is the number of bytes each builder can hold before it needs to allocate more memory.
const DEFAULT_ARRAY_BUILDER_BUFFER_SIZE: usize = 1024;

/// A [`ParquetUploader`] that writes rows to parquet files and uploads them to S3.
///
/// Spawns all S3 operations in tokio tasks to avoid blocking the surrounding timely context.
///
/// There are several layers of buffering in this uploader:
///
/// - The [`ArrowBuilder`] builds a structure of in-memory [`ColBuilder`]s from incoming
///   [`mz_repr::Row`]s. Each [`ColBuilder`] holds a specific [`arrow::array::builder`] type
///   for constructing a column of the given type. The entire [`ArrowBuilder`] is flushed to
///   the active [`ParquetFile`] by converting it into a [`RecordBatch`] once we've given it
///   more than [`DEFAULT_ARROW_BUILDER_BUFFER_SIZE`] row bytes.
///
/// - The [`ParquetFile`] holds a [`ArrowWriter`] that buffers until it has enough data to write
///   a parquet 'row group'. The 'row group' size is usually based on the number of rows (in the
///   ArrowWriter), but we also force it to flush based on data-size once it reaches the
///   [`PARQUET_ROW_GROUP_SIZE`].
///
/// - When a row group is written out, the active [`ParquetFile`] provides a reference to the row
///   group buffer to its [`S3MultiPartUploader`] which will copy the data to its own buffer.
///   If this upload buffer exceeds the configured part size limit, the [`S3MultiPartUploader`]
///   will upload parts to S3 until the upload buffer is below the limit.
///
/// - When the [`ParquetUploader`] is finished, it will flush the active [`ArrowBuilder`] and any
///   active [`ParquetFile`] which will in turn flush any open row groups to the
///   [`S3MultiPartUploader`] and upload the remaining parts to S3.
///      ┌───────────────┐
///      │ mz_repr::Rows │
///      └───────┬───────┘
///              │
/// ┌────────────▼────────────┐  ┌────────────────────────────┐
/// │       ArrowBuilder      │  │         ParquetFile        │
/// │                         │  │ ┌──────────────────┐       │
/// │     Vec<ArrowColumn>    │  │ │    ArrowWriter   │       │
/// │ ┌─────────┐ ┌─────────┐ │  │ │                  │       │
/// │ │         │ │         │ │  │ │   ┌──────────┐   │       │
/// │ │ColBuildr│ │ColBuildr│ ├──┼─┼──►│  buffer  │   │       │
/// │ │         │ │         │ │  │ │   └─────┬────┘   │       │
/// │ └─────────┘ └─────────┘ │  │ │         │        │       │
/// │                         │  │ │   ┌─────▼────┐   │       │
/// └─────────────────────────┘  │ │   │ row group│   │       │
///                              │ │   └─┬────────┘   │       │
///                              │ │     │            │       │
///                              │ └─────┼────────────┘       │
///                              │  ┌────┼──────────────────┐ │
///                              │  │    │       S3MultiPart│ │
///                              │  │ ┌──▼───────┐ Uploader │ │
///                              │  │ │  buffer  │          │ │
///      ┌─────────┐             │  │ └───┬─────┬┘          │ │
///      │ S3 API  │◄────────────┼──┤     │     │           │ │
///      └─────────┘             │  │ ┌───▼──┐ ┌▼─────┐     │ │
///                              │  │ │ part │ │ part │     │ │
///                              │  │ └──────┘ └──────┘     │ │
///                              │  │                       │ │
///                              │  └───────────────────────┘ │
///                              │                            │
///                              └────────────────────────────┘
pub(super) struct ParquetUploader {
    /// The output description.
    desc: RelationDesc,
    /// The index of the next file to upload within the batch.
    next_file_index: usize,
    /// Provides the appropriate bucket and object keys to use for uploads.
    key_manager: S3KeyManager,
    /// Identifies the batch that files uploaded by this uploader belong to.
    batch: u64,
    /// The aws sdk config.
    sdk_config: Arc<SdkConfig>,
    /// The active arrow builder.
    builder: ArrowBuilder,
    builder_max_size_bytes: u64,
    /// The active parquet file being written to, stored in an option
    /// since it won't be initialized until the builder is first flushed,
    /// and to make it easier to take ownership when calling in spawned
    /// tokio tasks (to avoid doing I/O in the surrounding timely context).
    active_file: Option<ParquetFile>,
}

impl CopyToS3Uploader for ParquetUploader {
    fn new(
        sdk_config: SdkConfig,
        connection_details: S3UploadInfo,
        sink_id: &GlobalId,
        batch: u64,
    ) -> Result<ParquetUploader, anyhow::Error> {
        match connection_details.format {
            S3SinkFormat::Parquet => {
                let builder = ArrowBuilder::new(
                    &connection_details.desc,
                    DEFAULT_ARRAY_BUILDER_ITEM_CAPACITY,
                    DEFAULT_ARRAY_BUILDER_BUFFER_SIZE,
                )?;
                Ok(ParquetUploader {
                    desc: connection_details.desc,
                    sdk_config: Arc::new(sdk_config),
                    key_manager: S3KeyManager::new(sink_id, &connection_details.uri),
                    batch,
                    next_file_index: 0,
                    builder,
                    builder_max_size_bytes: DEFAULT_ARROW_BUILDER_BUFFER_SIZE.as_u64(),
                    active_file: None,
                })
            }
            _ => anyhow::bail!("Expected Parquet format"),
        }
    }

    async fn append_row(&mut self, row: &Row) -> Result<(), anyhow::Error> {
        self.builder.add_row(row)?;

        if u64::cast_from(self.builder.row_size_bytes()) > self.builder_max_size_bytes {
            self.flush_builder().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        self.flush_builder().await?;
        if let Some(active) = self.active_file.take() {
            active.finish().await?;
        }
        Ok(())
    }
}

impl ParquetUploader {
    /// Start a new parquet file for upload. Will finish the current file if one is active.
    async fn start_new_file(&mut self) -> Result<(), anyhow::Error> {
        if let Some(active_file) = self.active_file.take() {
            active_file
                .finish()
                .run_in_task(|| "ParquetFile::finish")
                .await?;
        }
        let object_key = self
            .key_manager
            .data_key(self.batch, self.next_file_index, "parquet");
        self.next_file_index += 1;

        let schema = self.builder.schema();
        let bucket = self.key_manager.bucket.clone();
        let sdk_config = Arc::clone(&self.sdk_config);
        let new_file = ParquetFile::new(schema, bucket, object_key, sdk_config)
            .run_in_task(|| "ParquetFile::new")
            .await?;

        self.active_file = Some(new_file);
        Ok(())
    }

    /// Flush the current arrow builder to the active file. Starts a new file if the
    /// no file is currently active or if writing the current builder record batch to the
    /// active file would exceed the file size limit.
    async fn flush_builder(&mut self) -> Result<(), anyhow::Error> {
        let builder = std::mem::replace(
            &mut self.builder,
            ArrowBuilder::new(
                &self.desc,
                DEFAULT_ARRAY_BUILDER_ITEM_CAPACITY,
                DEFAULT_ARRAY_BUILDER_BUFFER_SIZE,
            )?,
        );
        let arrow_batch = builder.to_record_batch()?;

        if arrow_batch.num_rows() == 0 {
            return Ok(());
        }

        if self.active_file.is_none() {
            self.start_new_file().await?;
        }

        if let Some(mut active_file) = self.active_file.take() {
            let active_file = async move {
                active_file.write_arrow_batch(&arrow_batch).await?;
                Ok::<ParquetFile, anyhow::Error>(active_file)
            }
            .run_in_task(|| "ParquetFile::write_arrow_batch")
            .await?;
            self.active_file = Some(active_file);
        }
        Ok(())
    }
}

/// Helper to tie the lifecycle of the `ArrowWriter` and `S3MultiPartUploader` together
/// for a single parquet file.
struct ParquetFile {
    writer: ArrowWriter<Vec<u8>>,
    uploader: S3MultiPartUploader,
}

impl ParquetFile {
    async fn new(
        schema: Schema,
        bucket: String,
        key: String,
        sdk_config: Arc<SdkConfig>,
    ) -> Result<Self, anyhow::Error> {
        // TODO: Build a WriterProperties struct with the desired parquet file properties
        // and the correct buffering strategy (configuring the row group size, etc.).

        let writer = ArrowWriter::try_new(Vec::new(), schema.into(), None)?;
        let uploader = S3MultiPartUploader::try_new(
            sdk_config.as_ref(),
            bucket,
            key,
            S3MultiPartUploaderConfig {
                part_size_limit: UPLOAD_MULTIPART_PART_SIZE_LIMIT.as_u64(),
                // Set the max size enforced by the uploader to the max
                // file size it will allow based on the part size limit.
                file_size_limit: UPLOAD_MULTIPART_PART_SIZE_LIMIT
                    .as_u64()
                    .checked_mul(AWS_S3_MAX_PART_COUNT.try_into().expect("known safe"))
                    .expect("known safe"),
            },
        )
        .await?;
        Ok(Self { writer, uploader })
    }

    async fn finish(mut self) -> Result<CompletedUpload, anyhow::Error> {
        let buffer = self.writer.into_inner()?;
        self.uploader.buffer_chunk(buffer.as_slice()).await?;
        Ok(self.uploader.finish().await?)
    }

    /// Writes an arrow Record Batch to the parquet writer, then flushes the writer's buffer to
    /// the uploader which may trigger an upload.
    async fn write_arrow_batch(&mut self, record_batch: &RecordBatch) -> Result<(), anyhow::Error> {
        let before_groups = self.writer.flushed_row_groups().len();
        self.writer
            .write(record_batch)
            .map_err(|err| anyhow::anyhow!(err))?;

        // The writer will flush its buffer to a new parquet row group based on the row count,
        // not the actual size of the data. We flush manually to allow uploading the data in
        // potentially smaller chunks.
        if u64::cast_from(self.writer.in_progress_size()) > PARQUET_ROW_GROUP_SIZE.as_u64() {
            self.writer.flush().map_err(|err| anyhow::anyhow!(err))?;
        }

        // If the writer has flushed a new row group we can steal its buffer and upload it.
        if self.writer.flushed_row_groups().len() > before_groups {
            let buffer = self.writer.inner_mut();
            // TODO: this can return a S3MultiPartUploadError::UploadExceedsMaxFileLimit if we hit
            // the max S3 size limits. We should handle this case at some point and start a new
            // file, but for now this limit is high enough that it should rarely be hit and we
            // can just fail the upload.
            self.uploader.buffer_chunk(buffer.as_slice()).await?;
            // reuse the buffer in the writer
            buffer.clear();
        }
        Ok(())
    }
}
