//! The reads a data grid can replay page by page, and how one page of each
//! is fetched.

use dbflux_core::{
    CollectionBrowseRequest, Connection, ExecutionClassification, Pagination, QueryGenError,
    QueryGenerator, QueryResult, SelectQuery, TableBrowseRequest, VisualQuerySpec,
    classify_query_for_language,
};

use crate::pipeline::TransferError;

/// A read a data grid ran, kept so every row it would page through can be
/// read again with the same filter, sort and projection.
#[derive(Debug, Clone)]
pub enum ReplayableRead {
    /// A table browse. Paging starts at the table's first row, whatever page
    /// the request points at.
    Table(TableBrowseRequest),
    /// A collection browse. Paging starts at `skip`, the first document the
    /// grid's own first page shows.
    Collection {
        request: CollectionBrowseRequest,
        skip: u64,
    },
    /// A query builder SELECT, rendered by the driver's generator. Paging
    /// starts at the spec's offset and stops at its limit.
    VisualQuery {
        spec: VisualQuerySpec,
        database: Option<String>,
    },
}

/// One page of a read: the rows read, and how many were asked for.
pub(super) struct Page {
    pub result: QueryResult,
    pub requested: u64,
}

impl ReplayableRead {
    /// Reads `page_size` rows (fewer when a spec limit leaves fewer) starting
    /// `read_so_far` rows into the read. `None` when the spec limit is
    /// already reached, so no query runs.
    pub(super) fn fetch_page(
        &self,
        connection: &dyn Connection,
        page_size: u32,
        read_so_far: u64,
    ) -> Result<Option<Page>, TransferError> {
        let result = match self {
            Self::Table(request) => {
                let mut request = request.clone();
                request.pagination = Pagination::Offset {
                    limit: page_size,
                    offset: read_so_far,
                };
                connection.browse_table(&request)
            }
            Self::Collection { request, skip } => {
                let mut request = request.clone();
                request.pagination = Pagination::Offset {
                    limit: page_size,
                    offset: skip + read_so_far,
                };
                connection.browse_collection(&request)
            }
            Self::VisualQuery { spec, database } => {
                return fetch_visual_page(connection, spec, database, page_size, read_so_far);
            }
        };

        let result = result.map_err(|error| TransferError::Source(error.to_string()))?;
        Ok(Some(Page {
            result,
            requested: u64::from(page_size),
        }))
    }
}

fn fetch_visual_page(
    connection: &dyn Connection,
    spec: &VisualQuerySpec,
    database: &Option<String>,
    page_size: u32,
    read_so_far: u64,
) -> Result<Option<Page>, TransferError> {
    let remaining = spec.limit.map(|limit| limit.saturating_sub(read_so_far));
    let requested = match remaining {
        Some(0) => return Ok(None),
        Some(remaining) => remaining.min(u64::from(page_size)),
        None => u64::from(page_size),
    };

    let mut page_spec = spec.clone();
    page_spec.offset = spec.offset + read_so_far;
    page_spec.limit = Some(requested);

    let generator = connection.query_generator().ok_or_else(|| {
        TransferError::Source("the connection cannot render a builder query".to_string())
    })?;
    let select = select_for_visual_spec(generator, &page_spec)
        .map_err(TransferError::Source)?
        .ok_or_else(|| {
            TransferError::Source("the connection cannot render a builder query".to_string())
        })?;

    // The spec comes from the grid, not from a trusted source of reads, so
    // what the driver rendered is checked before it runs.
    let classification =
        classify_query_for_language(&connection.metadata().query_language, &select.sql);
    if !matches!(
        classification,
        ExecutionClassification::Read | ExecutionClassification::Metadata
    ) {
        return Err(TransferError::Source(format!(
            "the builder query is not a read ({classification:?}); it was not run"
        )));
    }

    let mut request = select.to_query_request(connection.dialect());
    request.database = database.clone();

    let result = connection
        .execute(&request)
        .map_err(|error| TransferError::Source(error.to_string()))?;

    Ok(Some(Page { result, requested }))
}

/// Renders the executable read for a builder spec.
///
/// Relational generators render a parameterized SELECT. A generator without
/// one (document drivers) falls back to `generate_read_from_spec`, whose
/// query text runs as a parameter-less SELECT. The choice follows which
/// method answers, never a driver id. `Ok(None)` when neither renders a
/// query; `Err` when the generator rejects the spec.
pub fn select_for_visual_spec(
    generator: &dyn QueryGenerator,
    spec: &VisualQuerySpec,
) -> Result<Option<SelectQuery>, String> {
    match generator.generate_select(spec) {
        Ok(Some(select)) => return Ok(Some(select)),
        Ok(None) | Err(QueryGenError::Unsupported) => {}
        Err(error) => return Err(error.to_string()),
    }

    match generator.generate_read_from_spec(spec) {
        Ok(Some(generated)) => Ok(Some(SelectQuery {
            sql: generated.text,
            params: Vec::new(),
        })),
        Ok(None) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}
