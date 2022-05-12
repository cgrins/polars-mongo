use polars::prelude::*;

use mongodb::bson::raw::{RawBsonRef, RawDocument};
use mongodb::bson::{Bson, Document};

#[derive(Debug)]
#[repr(transparent)]
pub struct Wrap<T>(pub T);

impl<T> Clone for Wrap<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Wrap(self.0.clone())
    }
}
impl<T> From<T> for Wrap<T> {
    fn from(t: T) -> Self {
        Wrap(t)
    }
}
// impl From<&RawDocument> for Wrap<DataType> {
//   fn from(doc: &RawDocument) -> Self {
//     let fields = doc.into_iter().map(|(key, value)| {
//       let dtype: Wrap<DataType> = value.into();

//       Field::new(key, dtype.0)
//     });

//     DataType::Struct(fields.collect()).into()
//   }
// }

impl From<RawBsonRef<'_>> for Wrap<DataType> {
    fn from(bson: RawBsonRef) -> Self {
        let dt = match bson {
            RawBsonRef::Double(_) => DataType::Float64,
            RawBsonRef::String(_) => DataType::Utf8,
            // RawBsonRef::Array(arr) => {
            //   // todo! add proper inference.
            //   let dt = arr.get(0).map(|i| {
            //     let dt: Self = i.into();
            //     dt.0
            //   }).unwrap_or(DataType::Null);

            //   DataType::List(Box::new(dt))
            // },
            RawBsonRef::Boolean(_) => DataType::Boolean,
            RawBsonRef::Null => DataType::Null,
            RawBsonRef::Int32(_) => DataType::Int32,
            RawBsonRef::Int64(_) => DataType::Int64,
            RawBsonRef::Timestamp(_) => DataType::Utf8,
            // RawBsonRef::Document(doc) => return *doc.into(),
            // RawBsonRef::Binary(_) => todo!(),
            RawBsonRef::DateTime(_) => DataType::Datetime(TimeUnit::Milliseconds, None),
            RawBsonRef::ObjectId(_) => DataType::Utf8,
            RawBsonRef::Symbol(_) => DataType::Utf8,
            RawBsonRef::Undefined => DataType::Unknown,
            _ => DataType::Utf8,
        };
        Wrap(dt)
    }
}
impl From<&RawDocument> for Wrap<Schema> {
    fn from(doc: &RawDocument) -> Self {
        let fields = doc.into_iter().map(|res| {
            let (key, value) = res.unwrap();

            let dtype: Wrap<DataType> = value.into();

            Field::new(key, dtype.0)
        });
        Schema::from(fields).into()
    }
}
// impl From<&RawDocument> for Wrap<Vec<(String, DataType)>> {
//   fn from(doc: &RawDocument) -> Self {
//     Wrap(doc.into_iter().map(|raw_doc| {
//       let (key, value) = raw_doc.unwrap();

//       let dtype: Wrap<DataType> = value.into();

//       (key, dtype.0)
//     }).collect())
//   }
// }

impl From<&Document> for Wrap<DataType> {
    fn from(doc: &Document) -> Self {
        let fields = doc.iter().map(|(key, value)| {
            let dtype: Wrap<DataType> = value.into();

            Field::new(key, dtype.0)
        });

        DataType::Struct(fields.collect()).into()
    }
}

impl<'a> From<&'a Document> for Wrap<AnyValue<'a>> {
    fn from(doc: &'a Document) -> Self {
        AnyValue::Utf8Owned(format!("{:#?}", doc)).into()

        // let fields = doc.iter().map(|(key, value)| {
        //   let dtype: Wrap<DataType> = value.into();

        //   Field::new(key, dtype.0)
        // }).collect();

        // let values = doc.iter().map(|(key, value)| {
        //   let av: Wrap<AnyValue> = value.into();
        //   av.0
        // }).collect();

        // AnyValue::StructOwned(Box::new((values, fields))).into()
    }
}
impl<'a> From<Document> for Wrap<AnyValue<'a>> {
    fn from(doc: Document) -> Self {
        // let fields = doc.iter().map(|(key, value)| {
        //   let dtype: Wrap<DataType> = value.into();

        //   Field::new(&key, dtype.0)
        // }).collect();

        // let values = doc.into_iter().map(|(key, value)| {
        //   let av: Wrap<AnyValue> = value.into();
        //   av.0
        // }).collect();
        AnyValue::Utf8Owned(format!("{:#?}", doc)).into()
        // AnyValue::StructOwned(Box::new((values, fields))).into()
    }
}

impl From<&Bson> for Wrap<DataType> {
    fn from(bson: &Bson) -> Self {
        let dt = match bson {
            Bson::Double(_) => DataType::Float64,
            Bson::String(_) => DataType::Utf8,
            Bson::Array(arr) => {
                // todo! add proper inference.
                let dt = arr
                    .get(0)
                    .map(|i| {
                        let dt: Self = i.into();
                        dt.0
                    })
                    .unwrap_or(DataType::Null);

                DataType::List(Box::new(dt))
            }
            Bson::Boolean(_) => DataType::Boolean,
            Bson::Null => DataType::Null,
            Bson::Int32(_) => DataType::Int32,
            Bson::Int64(_) => DataType::Int64,
            Bson::Timestamp(_) => DataType::Utf8,
            Bson::Document(doc) => return doc.into(),
            // Bson::Binary(_) => todo!(),
            Bson::DateTime(_) => DataType::Datetime(TimeUnit::Milliseconds, None),
            Bson::ObjectId(_) => DataType::Utf8,
            Bson::Symbol(_) => DataType::Utf8,
            Bson::Undefined => DataType::Unknown,
            _ => DataType::Utf8,
        };
        Wrap(dt)
    }
}
impl<'a> From<&'a Bson> for Wrap<AnyValue<'a>> {
    fn from(bson: &'a Bson) -> Self {
        let dt = match bson {
            Bson::Double(v) => AnyValue::Float64(*v),
            Bson::String(v) => AnyValue::Utf8(v),
            // Bson::Array(arr) => {
            //   let vals: Vec<Wrap<AnyValue>> = arr.iter().map(|v| v.into()).collect();

            //   // Wrap is transparent, so this is safe
            //   let vals = unsafe { std::mem::transmute::<_, Vec<AnyValue>>(vals) };
            //   let s = Series::new("", vals);
            //   AnyValue::List(s)
            //   // todo! add proper inference.
            //   // let dt = arr.get(0).map(|i| {
            //   //   let dt: Self = i.into();
            //   //   dt.0
            //   // }).unwrap_or(AnyValue::Null);

            //   // AnyValue::List(Box::new(dt))
            // }
            Bson::Boolean(b) => AnyValue::Boolean(*b),
            // Bson::Null => AnyValue::Null,
            Bson::Int32(v) => AnyValue::Int32(*v),
            Bson::Int64(v) => AnyValue::Int64(*v),
            Bson::Timestamp(v) => AnyValue::Utf8Owned(format!("{:#?}", v)),
            // Bson::Document(doc) => return doc.into(),
            // Bson::Binary(_) => todo!(),
            // Bson::DateTime(_) => AnyValue::Datetime(TimeUnit::Milliseconds, None),
            // Bson::ObjectId(_) => AnyValue::Utf8,
            // Bson::Symbol(_) => AnyValue::Utf8,
            // Bson::Undefined => AnyValue::Unknown,
            _ => AnyValue::Utf8("unhandled"),
        };
        Wrap(dt)
    }
}
impl<'a> From<Bson> for Wrap<AnyValue<'a>> {
    fn from(bson: Bson) -> Self {
        let dt = match bson {
            Bson::Double(v) => AnyValue::Float64(v),
            // Bson::String(v) => AnyValue::Utf8Owned(v),
            // Bson::Array(arr) => {
            //   let vals: Vec<Wrap<AnyValue>> = arr.iter().map(|v| v.into()).collect();

            //   // Wrap is transparent, so this is safe
            //   let vals = unsafe { std::mem::transmute::<_, Vec<AnyValue>>(vals) };
            //   let s = Series::new("", vals);
            //   AnyValue::List(s)
            //   // todo! add proper inference.
            //   // let dt = arr.get(0).map(|i| {
            //   //   let dt: Self = i.into();
            //   //   dt.0
            //   // }).unwrap_or(AnyValue::Null);

            //   // AnyValue::List(Box::new(dt))
            // }
            Bson::Boolean(b) => AnyValue::Boolean(b),
            // Bson::Null => AnyValue::Null,
            Bson::Int32(v) => AnyValue::Int32(v),
            Bson::Int64(v) => AnyValue::Int64(v),
            // Bson::Timestamp(v) => AnyValue::Utf8Owned(format!("{:#?}", v)),
            // Bson::Document(doc) => return doc.into(),
            // Bson::Binary(_) => todo!(),
            // Bson::DateTime(_) => AnyValue::Datetime(TimeUnit::Milliseconds, None),
            // Bson::ObjectId(_) => AnyValue::Utf8,
            // Bson::Symbol(_) => AnyValue::Utf8,
            // Bson::Undefined => AnyValue::Unknown,
            // _ => AnyValue::Null,
            _ => AnyValue::Utf8("unhandled"),
        };
        Wrap(dt)
    }
}
impl From<&Document> for Wrap<Schema> {
    fn from(doc: &Document) -> Self {
        let fields = doc.iter().map(|(key, value)| {
            let dtype: Wrap<DataType> = value.into();

            Field::new(key, dtype.0)
        });
        Schema::from(fields).into()
    }
}

impl From<&Document> for Wrap<Vec<(String, DataType)>> {
    fn from(doc: &Document) -> Self {
        let fields = doc.iter().map(|(key, value)| {
            let dtype: Wrap<DataType> = value.into();

            (key.clone(), dtype.0)
        });
        Wrap(fields.collect())
    }
}
