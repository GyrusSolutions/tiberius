use std::borrow::Cow;

use enumflags2::BitFlags;
use futures_util::io::{AsyncRead, AsyncWrite};

use crate::{
    tds::codec::{RpcParam, RpcStatus::ByRefValue, RpcValue},
    Client, ColumnData, CommandResult, IntoSql,
};

/// expected from a structure that represents a row, Derive macro to come
/// but actually it would make sense to pass &mut Command or &mut Param; looks overengineered
pub trait TableValueRow<'a, B>
where
    B: SqlValBound<'a> + 'a,
{
    fn bind_fields(&self, sql: &mut B);
    fn get_db_type() -> &'static str;
}

pub trait SqlValBound<'a> {
    fn bind(&mut self, val: impl IntoSql<'a> + 'a);
}

pub trait TableValue<'a> {
    fn bind_rows(self, sql: &mut Vec<SqlTableDataRow<'a>>);
    fn get_db_type(&self) -> &'static str;
}

impl<'a, R, C> TableValue<'a> for C
where
    R: TableValueRow<'a, SqlTableDataRow<'a>> + 'a,
    C: IntoIterator<Item = R>,
{
    fn bind_rows(self, sql: &mut Vec<SqlTableDataRow<'a>>) {
        for r in self.into_iter() {
            let mut sql_row = SqlTableDataRow(Vec::new());
            r.bind_fields(&mut sql_row);
            sql.push(sql_row);
        }
    }
    fn get_db_type(&self) -> &'static str {
        R::get_db_type()
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
pub struct SqlTableDataRow<'a>(Vec<ColumnData<'a>>);
impl<'a> SqlTableDataRow<'a> {
    pub fn add_field(&mut self, data: impl IntoSql<'a> + 'a) {
        self.0.push(data.into_sql());
    }
}

impl<'a> SqlValBound<'a> for SqlTableDataRow<'a> {
    fn bind(&mut self, val: impl IntoSql<'a> + 'a) {
        self.add_field(val);
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
    pub fn bind_table(&mut self, name: impl Into<Cow<'a, str>>, data: impl TableValue<'a> + 'a) {
        let mut rows = SqlTableData {
            rows: Vec::new(),
            db_type: data.get_db_type(),
        };
        data.bind_rows(&mut rows.rows);
        self.params.push(CommandParam {
            name: name.into(),
            out: false,
            data: CommandParamData::Table(rows),
        });
    }

    /// TODO: document non-query call
    pub async fn exec_nonquery<'b, S>(
        self,
        client: &'b mut Client<S>,
    ) -> crate::Result<CommandResult>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        client.connection.flush_stream().await?;

        // still 1-to-1 till TVP are coming
        let rpc_params = self
            .params
            .into_iter()
            .map(|p| RpcParam {
                name: p.name,
                flags: if p.out {
                    BitFlags::from_flag(ByRefValue)
                } else {
                    BitFlags::empty()
                },
                value: match p.data {
                    CommandParamData::Scalar(col) => RpcValue::Scalar(col),
                    CommandParamData::Table(_) => todo!(),
                },
            })
            .collect();

        client.rpc_run_command(self.name, rpc_params).await?;

        CommandResult::new(&mut client.connection).await
    }
}

