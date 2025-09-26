use std::io;
use std::process::ExitCode;

use std::path::Path;
use std::path::PathBuf;

use clap::Parser;

use rs_arrow_concat_keys2rbat::arrow;

use arrow::array::PrimitiveArray;
use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;

use rs_ints2arrow::sync::fs::filename2ints64be;
use rs_ints2arrow::sync::fs::filename2ints64le;

use rs_arrow_concat_keys2rbat::sync::SimpleBucketSource;
use rs_arrow_concat_keys2rbat::sync::UuidBigSerialsSource;
use rs_arrow_concat_keys2rbat::sync::UuidSource;

fn uuid2filename(
    base: &Path,
    bucket: uuid::Uuid,
    date: &str,
    bprefix: &str,
    dprefix: &str,
    basename: &str,
) -> PathBuf {
    base.join(format!("{bprefix}{bucket}"))
        .join(format!("{dprefix}{date}"))
        .join(basename)
}

struct BigSerialsSource {
    pub base: PathBuf,
    pub date: String,
    pub bprefix: String,
    pub dprefix: String,
    pub basename: String,
    pub little_endian: bool,
}

impl UuidBigSerialsSource for BigSerialsSource {
    fn get_serials(&self, bucket: u128) -> Result<PrimitiveArray<Int64Type>, io::Error> {
        let u: uuid::Uuid = uuid::Uuid::from_u128(bucket);
        let fname = uuid2filename(
            &self.base,
            u,
            &self.date,
            &self.bprefix,
            &self.dprefix,
            &self.basename,
        );
        let arr = match self.little_endian {
            true => filename2ints64le(&fname),
            false => filename2ints64be(&fname),
        };

        arr.map_err(|e| format!("unable to get serials {fname:?}: {e}"))
            .map_err(io::Error::other)
    }
}

fn dirent2uuid(dirent: &std::fs::DirEntry, prefix: &str) -> Result<uuid::Uuid, io::Error> {
    let obasename = dirent.file_name();
    let basename: &str = &obasename.to_string_lossy();
    let noprefix: &str = basename.strip_prefix(prefix).unwrap_or_default();
    str::parse(noprefix)
        .map_err(|e| format!("invalid uuid {noprefix}: {e}"))
        .map_err(io::Error::other)
}

struct UuidSourceFs {
    pub base: PathBuf,
    pub prefix: String,
    pub bkt_name: String,
    pub key_name: String,
}

impl UuidSource for UuidSourceFs {
    fn get_uuids(&self) -> Result<impl Iterator<Item = Result<u128, io::Error>>, io::Error> {
        let dirents = std::fs::read_dir(&self.base)?;
        let uuids =
            dirents.map(|rdirent| rdirent.and_then(|dirent| dirent2uuid(&dirent, &self.prefix)));
        let mapd = uuids.map(|ruuid| ruuid.map(|u| u.as_u128()));
        Ok(mapd)
    }
}

impl UuidSourceFs {
    fn to_batch(&self, b: &BigSerialsSource) -> Result<RecordBatch, io::Error> {
        let bucket_name: &str = &self.bkt_name;
        let key_name: &str = &self.key_name;
        SimpleBucketSource::to_batch::<_, Int64Type>(self, b, bucket_name, key_name)
    }
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(short, long)]
    pub base_dir_name: String,

    #[arg(short, long)]
    pub bucket_prefix: String,

    #[arg(short, long)]
    pub date_prefix: String,

    #[arg(short, long)]
    pub date: String,

    #[arg(short, long)]
    pub bucket_column_name: String,

    #[arg(short, long)]
    pub key_column_name: String,

    #[arg(short, long)]
    pub basename: String,

    #[arg(short, long, default_value_t = false)]
    pub little_endian: bool,
}

fn sub() -> Result<(), io::Error> {
    let a = Args::parse();
    let usf = UuidSourceFs {
        base: Path::new(&a.base_dir_name).into(),
        prefix: a.bucket_prefix.clone(),
        bkt_name: a.bucket_column_name,
        key_name: a.key_column_name,
    };
    let bs2 = BigSerialsSource {
        base: a.base_dir_name.into(),
        date: a.date,
        bprefix: a.bucket_prefix,
        dprefix: a.date_prefix,
        basename: a.basename,
        little_endian: a.little_endian,
    };
    let rb: RecordBatch = usf.to_batch(&bs2)?;
    println!("{rb:?}");
    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
