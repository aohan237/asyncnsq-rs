use async_std::fs::File;
use async_std::io;
use async_std::io::prelude::*;
use async_std::io::BufReader;
use serde::de::DeserializeOwned;
use std::fmt::Debug as FmtDebug;

/// toml reader utils which read config from toml async
pub async fn read_toml_config<T: FmtDebug + DeserializeOwned>(file: String) -> io::Result<T> {
    let f = File::open(file).await?;
    let mut reader = BufReader::new(f);
    let mut content = String::new();
    reader.read_to_string(&mut content).await?;
    dbg!(&content);
    let result: T = toml::from_str::<T>(content.as_str())?;
    dbg!(&result);
    Ok(result)
}
