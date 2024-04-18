// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use aws_types::sdk_config::SdkConfig;
use bytesize::ByteSize;
use mz_aws_util::s3_uploader::{
    CompletedUpload, S3MultiPartUploadError, S3MultiPartUploader, S3MultiPartUploaderConfig,
    AWS_S3_MAX_PART_COUNT,
};
use mz_ore::task::JoinHandleExt;
use mz_pgcopy::{encode_copy_format, CopyFormatParams};
use mz_repr::{GlobalId, RelationDesc, Row};
use mz_storage_types::sinks::{S3SinkFormat, S3UploadInfo};
use tracing::info;

use super::{CopyToS3Uploader, S3KeyManager};

/// Required state to upload batches to S3
pub(super) struct PgCopyUploader {
    /// The output description.
    desc: RelationDesc,
    /// Params to format the data.
    format: CopyFormatParams<'static>,
    /// The index of the current file within the batch.
    file_index: usize,
    /// Provides the appropriate bucket and object keys to use for uploads
    key_manager: S3KeyManager,
    /// Identifies the batch that files uploaded by this uploader belong to
    batch: u64,
    /// The aws sdk config.
    /// This is an option so that we can get an owned value later to move to a
    /// spawned tokio task.
    sdk_config: Option<SdkConfig>,
    /// Multi-part uploader for the current file.
    /// Keeping the uploader in an `Option` to later take owned value.
    current_file_uploader: Option<S3MultiPartUploader>,
    /// Temporary buffer to store the encoded bytes.
    /// Currently at a time this will only store one single encoded row
    /// before getting added to the `current_file_uploader`'s buffer.
    buf: Vec<u8>,
}

impl CopyToS3Uploader for PgCopyUploader {
    fn new(
        sdk_config: SdkConfig,
        connection_details: S3UploadInfo,
        sink_id: &GlobalId,
        batch: u64,
    ) -> Result<PgCopyUploader, anyhow::Error> {
        match connection_details.format {
            S3SinkFormat::PgCopy(format_params) => Ok(PgCopyUploader {
                desc: connection_details.desc,
                sdk_config: Some(sdk_config),
                format: format_params,
                key_manager: S3KeyManager::new(sink_id, &connection_details.uri),
                batch,
                file_index: 0,
                current_file_uploader: None,
                buf: Vec::new(),
            }),
            _ => anyhow::bail!("Expected PgCopy format"),
        }
    }

    /// Finishes any remaining in-progress upload.
    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        if let Some(uploader) = self.current_file_uploader.take() {
            // Moving the aws s3 calls onto tokio tasks instead of using timely runtime.
            let handle =
                mz_ore::task::spawn(|| "s3_uploader::finish", async { uploader.finish().await });
            let CompletedUpload {
                part_count,
                total_bytes_uploaded,
                bucket,
                key,
            } = handle.wait_and_assert_finished().await?;
            info!(
                "finished upload: bucket {}, key {}, bytes_uploaded {}, parts_uploaded {}",
                bucket, key, total_bytes_uploaded, part_count
            );
        }
        Ok(())
    }

    /// Appends the row to the in-progress upload where it is buffered till it reaches the configured
    /// `part_size_limit` after which the `S3MultiPartUploader` will upload that part. In case it will
    /// exceed the max file size of the ongoing upload, then a new `S3MultiPartUploader` for a new file will
    /// be created and the row data will be appended there.
    async fn append_row(&mut self, row: &Row) -> Result<(), anyhow::Error> {
        self.buf.clear();
        // encode the row and write to temp buffer.
        encode_copy_format(&self.format, row, self.desc.typ(), &mut self.buf)
            .map_err(|_| anyhow!("error encoding row"))?;

        if self.current_file_uploader.is_none() {
            self.start_new_file_upload().await?;
        }

        match self.upload_buffer().await {
            Ok(_) => Ok(()),
            Err(S3MultiPartUploadError::UploadExceedsMaxFileLimit(_)) => {
                // Start a multi part upload of next file.
                self.start_new_file_upload().await?;
                // Upload data for the new part.
                self.upload_buffer().await?;
                Ok(())
            }
            Err(e) => Err(e),
        }?;

        Ok(())
    }
}

impl PgCopyUploader {
    /// Creates the uploader for the next file and starts the multi part upload.
    async fn start_new_file_upload(&mut self) -> Result<(), anyhow::Error> {
        self.flush().await?;
        assert!(self.current_file_uploader.is_none());

        self.file_index += 1;
        let object_key =
            self.key_manager
                .data_key(self.batch, self.file_index, self.format.file_extension());
        let bucket = self.key_manager.bucket.clone();
        info!("starting upload: bucket {}, key {}", &bucket, &object_key);
        let sdk_config = self
            .sdk_config
            .take()
            .expect("sdk_config should always be present");
        // Moving the aws s3 calls onto tokio tasks instead of using timely runtime.
        let handle = mz_ore::task::spawn(|| "s3_uploader::try_new", async move {
            let uploader = S3MultiPartUploader::try_new(
                &sdk_config,
                bucket,
                object_key,
                S3MultiPartUploaderConfig {
                    part_size_limit: ByteSize::mib(10).as_u64(),
                    // Set the max size enforced by the uploader to the max
                    // file size it will allow based on the part size limit.
                    file_size_limit: ByteSize::mib(10)
                        .as_u64()
                        .checked_mul(AWS_S3_MAX_PART_COUNT.try_into().expect("known safe"))
                        .expect("known safe"),
                },
            )
            .await;
            (uploader, sdk_config)
        });
        let (uploader, sdk_config) = handle.wait_and_assert_finished().await;
        self.sdk_config = Some(sdk_config);
        self.current_file_uploader = Some(uploader?);
        Ok(())
    }

    async fn upload_buffer(&mut self) -> Result<(), S3MultiPartUploadError> {
        assert!(!self.buf.is_empty());
        assert!(self.current_file_uploader.is_some());

        let mut uploader = self.current_file_uploader.take().unwrap();
        // TODO: Make buf a Bytes so it can be cheaply cloned.
        let buf = std::mem::take(&mut self.buf);
        // Moving the aws s3 calls onto tokio tasks instead of using timely runtime.
        let handle = mz_ore::task::spawn(|| "s3_uploader::buffer_chunk", async move {
            let result = uploader.buffer_chunk(&buf).await;
            (uploader, buf, result)
        });
        let (uploader, buf, result) = handle.wait_and_assert_finished().await;
        self.current_file_uploader = Some(uploader);
        self.buf = buf;

        let _ = result?;
        Ok(())
    }
}

/// On CI, these tests are enabled by adding the scratch-aws-access plugin
/// to the `cargo-test` step in `ci/test/pipeline.template.yml` and setting
/// `MZ_S3_UPLOADER_TEST_S3_BUCKET` in
/// `ci/test/cargo-test/mzcompose.py`.
///
/// For a Materialize developer, to opt in to these tests locally for
/// development, follow the AWS access guide:
///
/// ```text
/// https://www.notion.so/materialize/AWS-access-5fbd9513dcdc4e11a7591e8caa5f63fe
/// ```
///
/// then running `source src/aws-util/src/setup_test_env_mz.sh`. You will also have
/// to run `aws sso login` if you haven't recently.
#[cfg(test)]
mod tests {
    use mz_pgcopy::CopyFormatParams;
    use mz_repr::{ColumnName, ColumnType, Datum, RelationType};
    use uuid::Uuid;

    use super::*;

    fn s3_bucket_path_for_test() -> Option<(String, String)> {
        let bucket = match std::env::var("MZ_S3_UPLOADER_TEST_S3_BUCKET") {
            Ok(bucket) => bucket,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return None;
            }
        };

        let prefix = Uuid::new_v4().to_string();
        let path = format!("cargo_test/{}/file", prefix);
        Some((bucket, path))
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/materialize/issues/18898
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    async fn test_upload() -> Result<(), anyhow::Error> {
        let sdk_config = mz_aws_util::defaults().load().await;
        let (bucket, path) = match s3_bucket_path_for_test() {
            Some(tuple) => tuple,
            None => return Ok(()),
        };
        let sink_id = GlobalId::User(123);
        let batch = 456;
        let typ: RelationType = RelationType::new(vec![ColumnType {
            scalar_type: mz_repr::ScalarType::String,
            nullable: true,
        }]);
        let column_names = vec![ColumnName::from("col1")];
        let desc = RelationDesc::new(typ, column_names.into_iter());
        let mut uploader = PgCopyUploader::new(
            sdk_config.clone(),
            S3UploadInfo {
                uri: format!("s3://{}/{}", bucket, path),
                desc,
                format: S3SinkFormat::PgCopy(CopyFormatParams::Csv(Default::default())),
            },
            &sink_id,
            batch,
        )?;
        let mut row = Row::default();
        row.packer().push(Datum::from("1234567"));
        uploader.append_row(&row).await?;

        row.packer().push(Datum::Null);
        uploader.append_row(&row).await?;

        row.packer().push(Datum::from("5678"));
        uploader.append_row(&row).await?;

        uploader.flush().await?;

        let s3_client = mz_aws_util::s3::new_client(&sdk_config);
        let first_file = s3_client
            .get_object()
            .bucket(bucket.clone())
            .key(format!(
                "{}/mz-{}-batch-{:04}-0001.csv",
                path, sink_id, batch
            ))
            .send()
            .await
            .unwrap();

        let body = first_file.body.collect().await.unwrap().into_bytes();
        let expected_body: &[u8] = b"1234567\n\n5678\n";
        assert_eq!(body, *expected_body);

        Ok(())
    }
}
