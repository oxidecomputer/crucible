use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;

use ErrorKind::NotFound;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

pub fn read_json_maybe<P, T>(file: P) -> Result<Option<T>>
where
    P: AsRef<Path>,
    for<'de> T: Deserialize<'de>,
{
    let file = file.as_ref();
    let mut f = match File::open(file) {
        Ok(f) => f,
        Err(e) if e.kind() == NotFound => return Ok(None),
        Err(e) => bail!("open {:?}: {:?}", file, e),
    };
    let mut buf = Vec::<u8>::new();
    f.read_to_end(&mut buf)
        .with_context(|| anyhow!("read {:?}", file))?;
    Ok(serde_json::from_slice(buf.as_slice())
        .with_context(|| anyhow!("parse {:?}", file))?)
}

pub fn read_json<P, T>(file: P) -> Result<T>
where
    P: AsRef<Path>,
    for<'de> T: Deserialize<'de>,
{
    let file = file.as_ref();
    Ok(read_json_maybe(file)?
        .ok_or_else(|| anyhow!("open {:?}: file not found", file))?)
}

pub fn write_json<P, T>(file: P, data: &T, clobber: bool) -> Result<()>
where
    P: AsRef<Path>,
    T: Serialize,
{
    let file = file.as_ref();
    let mut buf = serde_json::to_vec_pretty(data)?;
    buf.push(b'\n');
    let mut tmpf = NamedTempFile::new_in(file.parent().unwrap())?;
    tmpf.write_all(&buf)?;
    tmpf.flush()?;

    if clobber {
        tmpf.persist(file)?;
    } else {
        tmpf.persist_noclobber(file)?;
    }
    Ok(())
}

pub fn mkdir_for_file(file: &Path) -> Result<()> {
    Ok(std::fs::create_dir_all(file.parent().expect("file path"))?)
}
