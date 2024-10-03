use std::ops::DerefMut;

use mz_ore::future::InTask;
use mz_ore::now::SYSTEM_TIME;
use mz_sql_parser::ast::{CreateSourceConnection, SourceIncludeMetadata};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::inline::{InlinedConnection, IntoInlineConnection};
use mz_storage_types::connections::Connection;
use mz_storage_types::sources::load_generator::LoadGenerator;
use mz_storage_types::sources::GenericSourceConnection;

use crate::catalog::SessionCatalog;
use crate::kafka_util::KafkaSourceConfigOptionExtracted;
use crate::names::Aug;
use crate::plan::statement::ddl::load_generator_ast_to_generator;
use crate::plan::{PlanError, SourceReference, SourceReferences, StatementContext};

use super::error::{
    KafkaSourcePurificationError, MySqlSourcePurificationError, PgSourcePurificationError,
};

/// A client that allows determining all available source references and resolving
/// them to a user-specified source reference during purification.
pub(super) enum SourceReferenceClient<'a> {
    Postgres {
        client: &'a mz_postgres_util::Client,
        publication: &'a str,
    },
    MySql {
        conn: &'a mut mz_mysql_util::MySqlConn,
    },
    Kafka {
        topic: &'a str,
    },
    LoadGenerator {
        generator: &'a LoadGenerator,
    },
}

impl<'a> SourceReferenceClient<'a> {
    // /// Instantiate a `SourceReferenceClient` from a `CreateSourceConnection`.
    // pub(super) async fn from_create_source(
    //     source: &CreateSourceConnection<Aug>,
    //     scx: &StatementContext<'_>,
    //     catalog: &impl SessionCatalog,
    //     storage_configuration: &StorageConfiguration,
    //     include_metadata: &[SourceIncludeMetadata],
    // ) -> Result<Self, PlanError> {
    //     match source {
    //         CreateSourceConnection::Postgres {
    //             connection,
    //             options,
    //         }
    //         | CreateSourceConnection::Yugabyte {
    //             connection,
    //             options,
    //         } => {
    //             let connection = {
    //                 let item = scx.get_item_by_resolved_name(connection)?;
    //                 match item.connection().map_err(PlanError::from)? {
    //                     Connection::Postgres(connection) => {
    //                         connection.clone().into_inline_connection(&catalog)
    //                     }
    //                     _ => Err(PgSourcePurificationError::NotPgConnection(
    //                         scx.catalog.resolve_full_name(item.name()),
    //                     ))?,
    //                 }
    //             };

    //             let crate::plan::statement::PgConfigOptionExtracted { publication, .. } =
    //                 options.clone().try_into()?;
    //             let publication =
    //                 publication.ok_or(PgSourcePurificationError::ConnectionMissingPublication)?;

    //             // verify that we can connect upstream and snapshot publication metadata
    //             let config = connection
    //                 .config(
    //                     &storage_configuration.connection_context.secrets_reader,
    //                     storage_configuration,
    //                     InTask::No,
    //                 )
    //                 .await?;

    //             let client = config
    //                 .connect(
    //                     "postgres_purification",
    //                     &storage_configuration.connection_context.ssh_tunnel_manager,
    //                 )
    //                 .await?;

    //             Ok(SourceReferenceClient::Postgres {
    //                 client,
    //                 publication,
    //             })
    //         }

    //         CreateSourceConnection::MySql {
    //             connection,
    //             options: _,
    //         } => {
    //             let connection_item = scx.get_item_by_resolved_name(connection)?;
    //             let connection = match connection_item.connection()? {
    //                 Connection::MySql(connection) => {
    //                     connection.clone().into_inline_connection(&catalog)
    //                 }
    //                 _ => Err(MySqlSourcePurificationError::NotMySqlConnection(
    //                     scx.catalog.resolve_full_name(connection_item.name()),
    //                 ))?,
    //             };

    //             let config = connection
    //                 .config(
    //                     &storage_configuration.connection_context.secrets_reader,
    //                     storage_configuration,
    //                     InTask::No,
    //                 )
    //                 .await?;

    //             let conn = config
    //                 .connect(
    //                     "mysql purification",
    //                     &storage_configuration.connection_context.ssh_tunnel_manager,
    //                 )
    //                 .await?;

    //             Ok(SourceReferenceClient::MySQL { conn })
    //         }
    //         CreateSourceConnection::LoadGenerator { generator, options } => {
    //             let load_generator =
    //                 load_generator_ast_to_generator(&scx, generator, options, include_metadata)?;

    //             Ok(SourceReferenceClient::LoadGenerator {
    //                 generator: load_generator,
    //             })
    //         }
    //         CreateSourceConnection::Kafka {
    //             connection: _,
    //             options,
    //         } => {
    //             let extracted_options: KafkaSourceConfigOptionExtracted =
    //                 options.clone().try_into()?;

    //             let topic = extracted_options
    //                 .topic
    //                 .ok_or(KafkaSourcePurificationError::ConnectionMissingTopic)?;
    //             Ok(SourceReferenceClient::Kafka { topic })
    //         }
    //     }
    // }

    // /// Instantiate a `SourceReferenceClient` from a `GenericSourceConnection`.
    // pub(super) async fn from_existing_source(
    //     source: &GenericSourceConnection<InlinedConnection>,
    //     storage_configuration: &StorageConfiguration,
    // ) -> Result<Self, PlanError> {
    //     match source {
    //         GenericSourceConnection::Postgres(pg_source_connection) => {
    //             // Get PostgresConnection for generating subsources.
    //             let pg_connection = &pg_source_connection.connection;

    //             let config = pg_connection
    //                 .config(
    //                     &storage_configuration.connection_context.secrets_reader,
    //                     storage_configuration,
    //                     InTask::No,
    //                 )
    //                 .await?;

    //             let client = config
    //                 .connect(
    //                     "postgres_purification",
    //                     &storage_configuration.connection_context.ssh_tunnel_manager,
    //                 )
    //                 .await?;
    //             Ok(SourceReferenceClient::Postgres {
    //                 client,
    //                 publication: pg_source_connection.publication.clone(),
    //             })
    //         }
    //         GenericSourceConnection::MySql(mysql_source_connection) => {
    //             let mysql_connection = &mysql_source_connection.connection;
    //             let config = mysql_connection
    //                 .config(
    //                     &storage_configuration.connection_context.secrets_reader,
    //                     storage_configuration,
    //                     InTask::No,
    //                 )
    //                 .await?;

    //             let conn = config
    //                 .connect(
    //                     "mysql purification",
    //                     &storage_configuration.connection_context.ssh_tunnel_manager,
    //                 )
    //                 .await?;

    //             Ok(SourceReferenceClient::MySQL { conn })
    //         }
    //         GenericSourceConnection::LoadGenerator(load_gen_connection) => {
    //             Ok(SourceReferenceClient::LoadGenerator {
    //                 generator: load_gen_connection.load_generator.clone(),
    //             })
    //         }
    //         GenericSourceConnection::Kafka(kafka_source_connection) => {
    //             Ok(SourceReferenceClient::Kafka {
    //                 topic: kafka_source_connection.topic.clone(),
    //             })
    //         }
    //     }
    // }

    /// Get all available source references.
    pub(super) async fn get_source_references(&mut self) -> Result<SourceReferences, PlanError> {
        match self {
            SourceReferenceClient::Postgres {
                client,
                publication,
            } => {
                let tables = mz_postgres_util::publication_info(client, publication).await?;
                Ok(SourceReferences {
                    updated_at: SYSTEM_TIME(),
                    references: tables
                        .into_iter()
                        .map(|table| SourceReference {
                            name: table.name,
                            namespace: Some(table.namespace),
                            columns: table.columns.into_iter().map(|c| c.name).collect(),
                        })
                        .collect(),
                })
            }
            SourceReferenceClient::MySql { conn } => {
                let tables = mz_mysql_util::schema_info(
                    (*conn).deref_mut(),
                    &mz_mysql_util::SchemaRequest::All,
                )
                .await?;

                Ok(SourceReferences {
                    updated_at: SYSTEM_TIME(),
                    references: tables
                        .into_iter()
                        .map(|table| SourceReference {
                            name: table.name,
                            namespace: Some(table.schema_name),
                            columns: table
                                .columns
                                .into_iter()
                                .map(|column| column.name())
                                .collect(),
                        })
                        .collect(),
                })
            }
            SourceReferenceClient::Kafka { topic } => Ok(SourceReferences {
                updated_at: SYSTEM_TIME(),
                references: vec![SourceReference {
                    name: topic.to_string(),
                    namespace: None,
                    columns: vec![],
                }],
            }),
            SourceReferenceClient::LoadGenerator { generator } => {
                let mut references = generator
                    .views()
                    .into_iter()
                    .map(|(view, relation, _)| SourceReference {
                        name: view.to_string(),
                        namespace: Some(generator.schema_name().to_string()),
                        columns: relation.iter_names().map(|n| n.to_string()).collect(),
                    })
                    .collect::<Vec<_>>();

                if references.is_empty() {
                    // If there are no views then this load-generator just has a single output
                    // uses the load-generator's schema name.
                    references.push(SourceReference {
                        name: generator.schema_name().to_string(),
                        namespace: Some(generator.schema_name().to_string()),
                        columns: vec![],
                    });
                }

                Ok(SourceReferences {
                    updated_at: SYSTEM_TIME(),
                    references,
                })
            }
        }
    }
}
