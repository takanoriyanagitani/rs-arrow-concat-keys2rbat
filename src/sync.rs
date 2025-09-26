use std::io;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::FixedSizeBinaryDictionaryBuilder;
use arrow::array::PrimitiveArray;

use arrow::datatypes::ArrowDictionaryKeyType;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;

use arrow::datatypes::Int64Type;

use arrow::record_batch::RecordBatch;

pub trait SimpleKeysSource<const N: usize> {
    type Serial: ArrowPrimitiveType;
    fn get_keys(&self, bucket: [u8; N]) -> Result<PrimitiveArray<Self::Serial>, io::Error>;
}

pub fn buckets2batch<K, I, S, const N: usize>(
    ksource: &K,
    buckets: I,
    bucket_name: &str,
    key_name: &str,
) -> Result<RecordBatch, io::Error>
where
    K: SimpleKeysSource<N>,
    I: Iterator<Item = Result<[u8; N], io::Error>>,
    S: ArrowDictionaryKeyType,
{
    let mut buckets_bldr: FixedSizeBinaryDictionaryBuilder<S> =
        FixedSizeBinaryDictionaryBuilder::new(N as i32);
    let mut key_refs: Vec<Arc<dyn Array>> = vec![];

    for rbkt in buckets {
        let bucket: [u8; N] = rbkt?;
        let keys: PrimitiveArray<_> = ksource.get_keys(bucket)?;
        for _ in &keys {
            buckets_bldr.append(bucket).map_err(io::Error::other)?;
        }
        let ka = Arc::new(keys);
        key_refs.push(ka);
    }
    let refs: Vec<&dyn Array> = key_refs.iter().map(|a| a.as_ref()).collect();
    let concatenated = arrow::compute::concat(&refs).map_err(io::Error::other)?;
    let buckets = buckets_bldr.finish();

    let btyp: DataType = buckets.data_type().clone();
    let ktyp: DataType = K::Serial::DATA_TYPE;
    let sch = Schema::new(vec![
        Field::new(bucket_name, btyp, false),
        Field::new(key_name, ktyp, false),
    ]);
    RecordBatch::try_new(sch.into(), vec![Arc::new(buckets), concatenated])
        .map_err(io::Error::other)
}

pub trait SimpleBucketSource<const N: usize> {
    fn get_buckets(&self) -> Result<impl Iterator<Item = Result<[u8; N], io::Error>>, io::Error>;

    fn to_batch<K, S>(
        &self,
        ksource: &K,
        bucket_name: &str,
        key_name: &str,
    ) -> Result<RecordBatch, io::Error>
    where
        K: SimpleKeysSource<N>,
        S: ArrowDictionaryKeyType,
    {
        let buckets = self.get_buckets()?;
        buckets2batch::<K, _, S, N>(ksource, buckets, bucket_name, key_name)
    }
}

pub trait UuidBigSerialsSource {
    fn get_serials(&self, bucket: u128) -> Result<PrimitiveArray<Int64Type>, io::Error>;
}

impl<T> SimpleKeysSource<16> for T
where
    T: UuidBigSerialsSource,
{
    type Serial = Int64Type;
    fn get_keys(&self, bucket: [u8; 16]) -> Result<PrimitiveArray<Self::Serial>, io::Error> {
        let bucket_id: u128 = u128::from_be_bytes(bucket);
        self.get_serials(bucket_id)
    }
}

pub trait UuidSource {
    fn get_uuids(&self) -> Result<impl Iterator<Item = Result<u128, io::Error>>, io::Error>;
}

impl<T> SimpleBucketSource<16> for T
where
    T: UuidSource,
{
    fn get_buckets(&self) -> Result<impl Iterator<Item = Result<[u8; 16], io::Error>>, io::Error> {
        let uuids = self.get_uuids()?;
        Ok(uuids.map(|r| r.map(|u| u.to_be_bytes())))
    }
}
