use asynchronous_codec::BytesMut;
use bytes::BufMut;

use crate::Error;

use super::{Encode, TokenColMetaData};

const TVPTYPE: u8 = 0xF3;

pub struct TypeInfoTvp<'a> {
    scheema_name: String,
    type_name: String,
    columns: TokenColMetaData<'a>, // 'static???
}

impl<'a> Encode<BytesMut> for TypeInfoTvp<'a> {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        // TVPTYPE        =   %xF3
        // TVP_TYPE_INFO  =   TVPTYPE
        //                    TVP_TYPENAME
        //                    TVP_COLMETADATA
        //                    [TVP_ORDER_UNIQUE]
        //                    [TVP_COLUMN_ORDERING]
        //                    TVP_END_TOKEN
        //                    *TVP_ROW
        //                    TVP_END_TOKEN

        dst.put_u8(TVPTYPE);

        Ok(())
    }
}

impl<'a> TypeInfoTvp<'a> {}
