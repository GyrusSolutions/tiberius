use std::borrow::BorrowMut;

use asynchronous_codec::BytesMut;
use bytes::BufMut;

use crate::ColumnData;

use super::{BytesMutWithTypeInfo, Encode, MetaDataColumn};

const TVPTYPE: u8 = 0xF3;

#[derive(Debug)]
pub struct TypeInfoTvp<'a> {
    scheema_name: &'a str,
    db_type_name: &'a str,
    columns: Option<Vec<MetaDataColumn<'a>>>,
    data: Vec<Vec<ColumnData<'a>>>,
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

        dst.put_u8(TVPTYPE);
        put_b_varchar("", dst); // DB name
        put_b_varchar(self.scheema_name, dst);
        put_b_varchar(self.db_type_name, dst);

        if let Some(columns_metadata) = self.columns {
            dst.put_u16_le(columns_metadata.len() as u16);
            for col in columns_metadata {
                // TvpColumnMetaData = UserType
                //                     Flags
                //                     TYPE_INFO
                //                     ColName ; Column metadata instance
                dst.put_u32_le(0_u32);
                dst.put_u16_le(col.base.flags.bits());
                col.base.encode(dst)?;
                put_b_varchar("", dst);
                // put_b_varchar(col.col_name, dst);
            }
        } else {
            dst.put_u16_le(0xFFFF_u16); // TVP_NULL_TOKEN, server knows the type
        }

        dst.put_u8(0_u8); // TVP_END_TOKEN

        for row in self.data.into_iter() {
            dst.put_u8(0x01u8); // TVP_ROW_TOKEN = %x01
            for (_i, col) in row.into_iter().enumerate() {
                let mut dst_ti = BytesMutWithTypeInfo::new(dst);
                // if let Some(ref metadata) = self.columns {
                //     dst_ti = dst_ti.with_type_info(&metadata[i].base.ty);
                // }
                col.encode(&mut dst_ti)?;
            }
        }
        // TVP_ROW_TOKEN = %x01 ; A row as defined by TVP_COLMETADATA follows
        // TvpColumnData = TYPE_VARBYTE ; Actual value must match metadata for the column
        // AllColumnData = *TvpColumnData ; Chunks of data, one per non-default column defined
        //                                ; in TVP_COLMETADATA
        // TVP_ROW       = TVP_ROW_TOKEN
        //                 AllColumnData

        dst.put_u8(0_u8); // TVP_END_TOKEN

        dbg!(dst);

        Ok(())
    }
}

fn put_b_varchar<T: AsRef<str>>(s: T, dst: &mut BytesMut) {
    let len_pos = dst.len();
    dst.put_u8(0u8);
    let mut length = 0_u8;

    for chr in s.as_ref().encode_utf16() {
        dst.put_u16_le(chr);
        length += 1;
    }
    let dst: &mut [u8] = dst.borrow_mut();
    dst[len_pos] = length;
}

impl<'a> TypeInfoTvp<'a> {
    pub fn new(type_name: &'a str, rows: Vec<Vec<ColumnData<'a>>>) -> TypeInfoTvp<'a> {
        let (scheema_name, db_type_name) = if let Some((s, t)) = type_name.split_once(".") {
            (s, t)
        } else {
            ("", type_name.as_ref())
        };
        TypeInfoTvp {
            scheema_name,
            db_type_name,
            columns: None,
            data: rows,
        }
    }

    pub fn with_metadata(self, metadata: Vec<MetaDataColumn<'a>>) -> TypeInfoTvp<'_> {
        TypeInfoTvp {
            columns: Some(metadata),
            ..self
        }
    }
}
