use futures_util::stream::{BoxStream, TryStreamExt};

use super::ReceivedToken;

pub(crate) struct TokenFilterStream<'a, C>
where
    C: Fn(&ReceivedToken) + Send + Sync,
{
    input_stream: BoxStream<'a, crate::Result<ReceivedToken>>,
    cb: &'a C,
}

impl<'a, C> TokenFilterStream<'a, C>
where
    C: Fn(&ReceivedToken) + Send + Sync,
{
    pub(crate) fn new(
        input_stream: BoxStream<'a, crate::Result<ReceivedToken>>,
        cb: &'a C,
    ) -> Self {
        Self { input_stream, cb }
    }

    pub fn try_unfold(self) -> BoxStream<'a, crate::Result<ReceivedToken>> {
        let stream = futures_util::stream::try_unfold(self, |mut this| async move {
            let token = this.input_stream.try_next().await?;

            if let Some(token) = token {
                (this.cb)(&token);
                return Ok(Some((token, this)));
            }
            return Ok(None);
        });

        Box::pin(stream)
    }
}
