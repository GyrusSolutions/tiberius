use std::borrow::Cow;

use enumflags2::BitFlags;
use futures_util::io::{AsyncRead, AsyncWrite};

use crate::{tds::codec::RpcParam, Client, ColumnData, CommandResult, IntoSql};

/// temporary encapsulation for the experimental stuff
#[derive(Debug)]
pub struct Command<'a> {
    name: Cow<'a, str>,
    params: Vec<CommandParam<'a>>,
}

#[derive(Debug)]
struct CommandParam<'a> {
    name: Cow<'a, str>,
    _out: bool,
    data: ColumnData<'a>,
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
            _out: false,
            data: data.into_sql(),
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

        // 1-to-1 till TVP/output are coming
        let rpc_params = self
            .params
            .into_iter()
            .map(|p| RpcParam {
                name: p.name,
                flags: BitFlags::empty(),
                value: p.data,
            })
            .collect();

        client.rpc_run_command(self.name, rpc_params).await?;

        CommandResult::new(&mut client.connection).await
    }
}
