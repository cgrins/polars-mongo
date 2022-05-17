#![deny(clippy::all)]

use crate::conversion::Wrap;
use bson::buffer::{init_buffers, Buffer};
use futures::stream::TryStreamExt;
use polars::{frame::row::*, prelude::*};

pub mod bson;
pub mod conversion;

use mongodb::{
    bson::{doc, Bson, Document},
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

    pub async fn infer_schema(
        &self,
        infer_schema_len: usize,
        projection: &Option<Document>,
    ) -> Result<Schema> {
        let mut infer_options = FindOptions::builder()
            .limit(Some(infer_schema_len as i64))
            .build();
        infer_options.projection = projection.clone();

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
        Ok(schema)
    }

    pub async fn read<S, I>(
        &self,
        infer_schema_len: usize,
        limit: usize,
        columns: Option<I>,
    ) -> Result<DataFrame>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let projection = columns.map(cols_to_mongo_projection);

        let schema = self
            .infer_schema(infer_schema_len, &projection)
            .await
            .unwrap();

        let mut limit_options = FindOptions::builder().limit(Some(limit as i64)).build();
        limit_options.projection = projection;

        let mut buffers = init_buffers(&schema, limit)?;
        let cursor = self
            .collection
            .find(None, Some(limit_options))
            .await
            .unwrap();

        parse_lines(cursor, &mut buffers).await.unwrap();

        DataFrame::new(
            buffers
                .into_values()
                .map(|buf| buf.into_series())
                .collect::<Result<_>>()?,
        )
    }
}

fn cols_to_mongo_projection<S, I>(cols: I) -> Document
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let prj = cols.into_iter().map(|col| {
        let col = col.as_ref().to_string();
        (col, Bson::Int64(1))
    });
    Document::from_iter(prj)
}
async fn parse_lines<'a>(
    mut cursor: Cursor<Document>,
    buffers: &mut PlHashMap<String, Buffer<'a>>,
) -> mongodb::error::Result<()> {
    while let Some(doc) = cursor.try_next().await? {
        buffers.iter_mut().for_each(|(s, inner)| match doc.get(s) {
            Some(v) => inner.add(v).expect("inner.add(v)"),
            None => inner.add_null(),
        });
    }
    Ok(())
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
        let df = reader.read(100, 10000, Some(vec!["ticker", "peers", "address"])).await.unwrap();
        println!("{df}");

        assert_eq!(true, false)
    }
}
