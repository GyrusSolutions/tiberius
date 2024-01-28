use std::borrow::Cow;

use enumflags2::BitFlags;
use futures_util::io::{AsyncRead, AsyncWrite};

use crate::{
    tds::{
        codec::{RpcParam, RpcStatus::ByRefValue, RpcValue, TypeInfoTvp},
        stream::{CommandStream, TokenStream},
    },
    Client, ColumnData, IntoSql,
};

/// expected from a structure that represents a row, Derive macro to come
pub trait TableValueRow<'a> {
    /// TODO: document bind_fields
    fn bind_fields(&self, data_row: &mut SqlTableDataRow<'a>); // call data_row.add_field(val) for each field
    /// TODO: document get_db_type
    fn get_db_type() -> &'static str; // "dbo.MyType", macro-generated
}

pub trait TableValue<'a> {
    fn into_sql(self) -> SqlTableData<'a>;
}

impl<'a, R, C> TableValue<'a> for C
where
    R: TableValueRow<'a> + 'a,
    C: IntoIterator<Item = R>,
{
    fn into_sql(self) -> SqlTableData<'a> {
        let mut data = Vec::new();
        for row in self.into_iter() {
            let mut data_row = SqlTableDataRow::new();
            row.bind_fields(&mut data_row);
            data.push(data_row);
        }

        SqlTableData {
            rows: data,
            db_type: R::get_db_type(),
        }
    }
}

/// temporary encapsulation for the experimental stuff
#[derive(Debug)]
pub struct Command<'a> {
    name: Cow<'a, str>,
    params: Vec<CommandParam<'a>>, // TODO: might make sense to check if param names are unique, but server would recject repeating params anyway
}

#[derive(Debug)]
struct CommandParam<'a> {
    name: Cow<'a, str>,
    out: bool,
    data: CommandParamData<'a>, //ColumnData<'a>,
}

#[derive(Debug)]
enum CommandParamData<'a> {
    Scalar(ColumnData<'a>),
    Table(SqlTableData<'a>),
}

#[derive(Debug)]
pub struct SqlTableData<'a> {
    rows: Vec<SqlTableDataRow<'a>>,
    db_type: &'static str,
}

#[derive(Debug)]
/// TVP row binding public API
pub struct SqlTableDataRow<'a> {
    col_data: Vec<ColumnData<'a>>,
}
impl<'a> SqlTableDataRow<'a> {
    fn new() -> SqlTableDataRow<'a> {
        SqlTableDataRow {
            col_data: Vec::new(),
        }
    }
    /// Adds TVP field value to the row
    pub fn add_field(&mut self, data: impl IntoSql<'a> + 'a) {
        self.col_data.push(data.into_sql());
    }
}

impl<'a> Command<'a> {
    /// TODO: document new Command instance, proc name must be provided
    pub fn new(proc_name: impl Into<Cow<'a, str>>) -> Self {
        Self {
            name: proc_name.into(),
            params: Vec::new(),
        }
    }

    /// TODO: document bind scalar param val
    pub fn bind_param(&mut self, name: impl Into<Cow<'a, str>>, data: impl IntoSql<'a> + 'a) {
        self.params.push(CommandParam {
            name: name.into(),
            out: false,
            data: CommandParamData::Scalar(data.into_sql()),
        });
    }

    /// TODO: document bind scalar param val
    pub fn bind_out_param(&mut self, name: impl Into<Cow<'a, str>>, data: impl IntoSql<'a> + 'a) {
        self.params.push(CommandParam {
            name: name.into(),
            out: true,
            data: CommandParamData::Scalar(data.into_sql()),
        });
    }

    /// TODO: document bind table param val
    #[cfg(feature = "tds73")]
    pub fn bind_table(&mut self, name: impl Into<Cow<'a, str>>, data: impl TableValue<'a> + 'a) {
        self.params.push(CommandParam {
            name: name.into(),
            out: false,
            data: CommandParamData::Table(data.into_sql()),
        });
    }

    /// TODO: document query call
    pub async fn exec<'b, S>(self, client: &'b mut Client<S>) -> crate::Result<CommandStream<'b>>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let rpc_params = Command::build_rpc_params(self.params, client).await?;

        client.connection.flush_stream().await?;
        client.rpc_run_command(self.name, rpc_params).await?;

        let ts = TokenStream::new(&mut client.connection);
        let result = CommandStream::new(ts.try_unfold());

        Ok(result)
    }

    async fn build_rpc_params<'b, S>(
        cmd_params: Vec<CommandParam<'a>>,
        client: &'b mut Client<S>,
    ) -> crate::Result<Vec<RpcParam<'a>>>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let mut rpc_params = Vec::new();
        for p in cmd_params.into_iter() {
            let rpc_val = match p.data {
                CommandParamData::Scalar(col) => RpcValue::Scalar(col),
                CommandParamData::Table(t) => {
                    let type_info_tvp = TypeInfoTvp::new(
                        t.db_type,
                        t.rows.into_iter().map(|r| r.col_data).collect(),
                    );
                    // it might make sense to expose some API for the caller so they could cache metadata
                    let cols_metadata = client
                        .query_run_for_metadata(format!(
                            "DECLARE @P AS {};SELECT TOP 0 * FROM @P",
                            t.db_type
                        ))
                        .await?;
                    RpcValue::Table(if let Some(cm) = cols_metadata {
                        type_info_tvp.with_metadata(cm)
                    } else {
                        type_info_tvp
                    })
                }
            };
            let rpc_param = RpcParam {
                name: p.name,
                flags: if p.out {
                    BitFlags::from_flag(ByRefValue)
                } else {
                    BitFlags::empty()
                },
                value: rpc_val,
            };
            rpc_params.push(rpc_param);
        }
        Ok(rpc_params)
    }
}
