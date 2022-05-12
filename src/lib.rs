#![deny(clippy::all)]

use crate::conversion::Wrap;
use bson::buffer::{init_buffers, Buffer};
use futures::stream::TryStreamExt;
use polars::{frame::row::*, prelude::*};
use rayon::prelude::*;

// use std::fmt::Debug;
pub mod bson;
pub mod conversion;

use mongodb::{
    bson::{doc, Document},
    options::{ClientOptions, FindOptions},
    Client, Cursor,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TableOptions {
    pub connection_str: String,
    pub db: String,
    pub collection: String,
}

#[derive(Debug)]
pub struct MongoReader {
    collection: mongodb::Collection<Document>,
}

impl MongoReader {
    pub async fn connect(options: &TableOptions) -> Result<Self> {
        let client_options = ClientOptions::parse(&options.connection_str)
            .await
            .map_err(|_| {
                PolarsError::ComputeError(format!("Unable parse options: {:#?}", options).into())
            })?;
        let client = Client::with_options(client_options).map_err(|_| {
            PolarsError::ComputeError(
                format!("Unable build cliet from options: {:#?}", options).into(),
            )
        })?;

        let database = client.database(&options.db);
        let collection = database.collection::<Document>(&options.collection);
        Ok(MongoReader { collection })
    }

    pub async fn infer_schema(&self, infer_schema_len: usize) -> Result<Schema> {
        let infer_options = FindOptions::builder()
            .limit(Some(infer_schema_len as i64))
            .build();
        let inferable: Vec<Vec<(String, DataType)>> = self
            .collection
            .find(None, Some(infer_options))
            .await
            .map_err(|_| {
                PolarsError::ComputeError("Unable to fetch rows for determining schema".into())
            })?
            .map_ok(|f| {
                let v = f.into_iter().map(|(key, value)| {
                    let dtype: Wrap<DataType> = (&value).into();

                    (key, dtype.0)
                });
                v.collect()
            })
            .try_collect()
            .await
            .map_err(|_| {
                PolarsError::ComputeError("Unable to fetch rows for determining schema".into())
            })?;

        let schema = infer_schema(inferable.into_iter(), 1);
        println!("Schema={:#?}", schema);
        Ok(schema)
    }

    async fn read(&self) -> Result<DataFrame> {
        let schema = self.infer_schema(10).await.unwrap();
        let mut buffers = init_buffers(&schema, 1000)?;

        let mut cursor = self.collection.find(None, None).await.unwrap();

        parse_lines(cursor, &mut buffers).await.unwrap();

        DataFrame::new(
            buffers
                .into_values()
                .map(|buf| buf.into_series())
                .collect::<Result<_>>()?,
        )
    }
}

async fn parse_lines(
    mut cursor: Cursor<Document>,
    buffers: &mut PlHashMap<String, Buffer>,
) -> mongodb::error::Result<()> {
    while let Some(doc) = cursor.try_next().await? {
        buffers.iter_mut().for_each(|(s, inner)| match doc.get(s) {
            Some(v) => inner.add_null(),
            None => inner.add_null(),
        });
    }

    todo!()
    // let mut stream = Deserializer::from_slice(bytes).into_iter::<Value>();
    // for value in stream.by_ref() {
    //     let v = value.unwrap_or(Value::Null);
    //     match v {
    //         Value::Object(value) => {
    //             buffers
    //                 .iter_mut()
    //                 .for_each(|(s, inner)| match value.get(s) {
    //                     Some(v) => {
    //                         inner.add(v).expect("inner.add(v)");
    //                     }
    //                     None => inner.add_null(),
    //                 });
    //         }
    //         _ => {
    //             buffers.iter_mut().for_each(|(_, inner)| inner.add_null());
    //         }
    //     };
    // }

    // let byte_offset = stream.byte_offset();

    // Ok(byte_offset)
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_connect() {
        let file = std::fs::File::open("./config.json").unwrap();
        let options: TableOptions = serde_json::from_reader(file).unwrap();
        let reader = MongoReader::connect(&options).await.unwrap();
        let schema = reader.infer_schema(10).await.unwrap();
        dbg!(schema);
        reader.read().await.unwrap();

        assert_eq!(true, false)
        // println!("{:#?}", schema);
    }
}
