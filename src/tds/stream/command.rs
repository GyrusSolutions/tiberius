use crate::tds::stream::ReceivedToken;
use crate::{row::ColumnType, Column, Row};
use crate::{ColumnData, CommandResult, ResultMetadata};
use futures_util::{
    ready,
    stream::{BoxStream, Peekable, Stream, StreamExt, TryStreamExt},
};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

pub struct CommandStream<'a> {
    token_stream: Peekable<BoxStream<'a, crate::Result<ReceivedToken>>>,
    columns: Option<Arc<Vec<Column>>>,
    result_set_index: Option<usize>,
}

impl<'a> Debug for CommandStream<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandStream")
            .field(
                "token_stream",
                &"BoxStream<'a, crate::Result<ReceivedToken>>",
            )
            .finish()
    }
}

impl<'a> CommandStream<'a> {
    pub(crate) fn new(token_stream: BoxStream<'a, crate::Result<ReceivedToken>>) -> Self {
        Self {
            token_stream: token_stream.peekable(),
            columns: None,
            result_set_index: None,
        }
    }

    /// Collects all results from the command in the stream into memory in the order
    /// of querying.
    pub async fn into_command_result(mut self) -> crate::Result<CommandResult> {
        let mut results: Vec<Vec<Row>> = Vec::new();
        let mut result: Option<Vec<Row>> = None;
        let mut return_status = 0;
        let mut return_values = Vec::new();
        let mut rows_affected = Vec::new();

        while let Some(item) = self.try_next().await? {
            match (item, &mut result) {
                (CommandItem::Row(row), None) => {
                    result = Some(vec![row]);
                }
                (CommandItem::Row(row), Some(ref mut result)) => result.push(row),
                (CommandItem::Metadata(_), None) => {
                    result = Some(Vec::new());
                }
                (CommandItem::Metadata(_), ref mut previous_result) => {
                    results.push(previous_result.take().unwrap());
                    result = None;
                }
                (CommandItem::ReturnStatus(rs), _) => return_status = rs,
                (CommandItem::ReturnValue(rv), _) => return_values.push(rv),
                (CommandItem::RowsAffected(rows), _) => rows_affected.push(rows),
            }
        }

        if let Some(result) = result {
            results.push(result);
        }

        Ok(CommandResult {
            return_code: return_status,
            return_values,
            query_results: results,
            rows_affected,
        })
    }

    /// Convert the stream into a stream of rows, skipping all other items.
    pub fn into_row_stream(self) -> BoxStream<'a, crate::Result<Row>> {
        let s = self.try_filter_map(|item| async {
            match item {
                CommandItem::Row(row) => Ok(Some(row)),
                _ => Ok(None),
            }
        });

        Box::pin(s)
    }
}

#[derive(Debug)]
pub struct CommandReturnValue {
    pub(crate) name: String,
    pub(crate) _ord: u16, // TODO: remove? do we need it?
    pub(crate) data: ColumnData<'static>,
}

/// Resulting data from a command.
#[derive(Debug)]
pub enum CommandItem {
    /// A single row of data.
    Row(Row),
    /// Information of the upcoming row data.
    Metadata(ResultMetadata),
    /// Return Status from the server
    ReturnStatus(u32),
    /// Return Value, matching OUT parameter(s)
    ReturnValue(CommandReturnValue),
    /// Rows Affected, for one of the statements ran in server
    RowsAffected(u64),
}

impl CommandItem {
    pub(crate) fn metadata(columns: Arc<Vec<Column>>, result_index: usize) -> Self {
        Self::Metadata(ResultMetadata {
            columns,
            result_index,
        })
    }

    /// Returns a reference to the metadata, if the item is of a correct variant.
    pub fn as_metadata(&self) -> Option<&ResultMetadata> {
        match self {
            CommandItem::Metadata(ref metadata) => Some(metadata),
            _ => None,
        }
    }

    /// Returns a reference to the row, if the item is of a correct variant.
    pub fn as_row(&self) -> Option<&Row> {
        match self {
            CommandItem::Row(ref row) => Some(row),
            _ => None,
        }
    }

    /// Returns the metadata, if the item is of a correct variant.
    pub fn into_metadata(self) -> Option<ResultMetadata> {
        match self {
            CommandItem::Metadata(metadata) => Some(metadata),
            _ => None,
        }
    }

    /// Returns the row, if the item is of a correct variant.
    pub fn into_row(self) -> Option<Row> {
        match self {
            CommandItem::Row(row) => Some(row),
            _ => None,
        }
    }
}

impl<'a> Stream for CommandStream<'a> {
    type Item = crate::Result<CommandItem>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            let token = match ready!(this.token_stream.poll_next_unpin(cx)) {
                Some(res) => res?,
                None => return Poll::Ready(None),
            };

            return match token {
                ReceivedToken::NewResultset(meta) => {
                    let column_meta = meta
                        .columns
                        .iter()
                        .map(|x| Column {
                            name: x.col_name.to_string(),
                            column_type: ColumnType::from(&x.base.ty),
                        })
                        .collect::<Vec<_>>();

                    let column_meta = Arc::new(column_meta);
                    this.columns = Some(column_meta.clone());

                    this.result_set_index = this.result_set_index.map(|i| i + 1);

                    let query_item =
                        CommandItem::metadata(column_meta, *this.result_set_index.get_or_insert(0));

                    return Poll::Ready(Some(Ok(query_item)));
                }
                ReceivedToken::Row(data) => {
                    let columns = this.columns.as_ref().unwrap().clone();
                    let result_index = this.result_set_index.unwrap();

                    let row = Row {
                        columns,
                        data,
                        result_index,
                    };

                    Poll::Ready(Some(Ok(CommandItem::Row(row))))
                }
                ReceivedToken::ReturnStatus(rs) => {
                    Poll::Ready(Some(Ok(CommandItem::ReturnStatus(rs))))
                }
                ReceivedToken::ReturnValue(rv) => {
                    Poll::Ready(Some(Ok(CommandItem::ReturnValue(CommandReturnValue {
                        name: rv.param_name,
                        _ord: rv.param_ordinal,
                        data: rv.value,
                    }))))
                }
                ReceivedToken::DoneProc(done) if done.is_final() => continue,
                ReceivedToken::DoneProc(done) => {
                    Poll::Ready(Some(Ok(CommandItem::RowsAffected(done.rows()))))
                }
                ReceivedToken::DoneInProc(done) => {
                    Poll::Ready(Some(Ok(CommandItem::RowsAffected(done.rows()))))
                }
                ReceivedToken::Done(done) => {
                    Poll::Ready(Some(Ok(CommandItem::RowsAffected(done.rows()))))
                }
                _ => continue,
            };
        }
    }
}
