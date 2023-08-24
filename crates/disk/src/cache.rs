use chrono::Utc;
use err::Error;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::AggKind;
use netpod::Cluster;
use netpod::NodeConfigCached;
use netpod::PreBinnedPatchCoordEnum;
use netpod::SfDbChannel;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;
use std::io;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tiny_keccak::Hasher;

// For file-based caching, this determined the node where the cache file is located.
// No longer needed for scylla-based caching.
pub fn node_ix_for_patch(patch_coord: &PreBinnedPatchCoordEnum, channel: &SfDbChannel, cluster: &Cluster) -> u32 {
    let mut hash = tiny_keccak::Sha3::v256();
    hash.update(channel.backend().as_bytes());
    hash.update(channel.name().as_bytes());
    /*hash.update(&patch_coord.patch_beg().to_le_bytes());
    hash.update(&patch_coord.patch_end().to_le_bytes());
    hash.update(&patch_coord.bin_t_len().to_le_bytes());
    hash.update(&patch_coord.patch_t_len().to_le_bytes());*/
    let mut out = [0; 32];
    hash.finalize(&mut out);
    let a = [out[0], out[1], out[2], out[3]];
    let ix = u32::from_le_bytes(a) % cluster.nodes.len() as u32;
    ix
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheFileDesc {
    // What identifies a cached file?
    channel: SfDbChannel,
    patch: PreBinnedPatchCoordEnum,
    agg_kind: AggKind,
}

impl CacheFileDesc {
    pub fn new(channel: SfDbChannel, patch: PreBinnedPatchCoordEnum, agg_kind: AggKind) -> Self {
        Self {
            channel,
            patch,
            agg_kind,
        }
    }

    pub fn hash(&self) -> String {
        let mut h = tiny_keccak::Sha3::v256();
        h.update(b"V000");
        h.update(self.channel.backend().as_bytes());
        h.update(self.channel.name().as_bytes());
        h.update(format!("{}", self.agg_kind).as_bytes());
        //h.update(&self.patch.spec().bin_t_len().to_le_bytes());
        //h.update(&self.patch.spec().patch_t_len().to_le_bytes());
        //h.update(&self.patch.ix().to_le_bytes());
        let mut buf = [0; 32];
        h.finalize(&mut buf);
        hex::encode(&buf)
    }

    pub fn hash_channel(&self) -> String {
        let mut h = tiny_keccak::Sha3::v256();
        h.update(b"V000");
        h.update(self.channel.backend().as_bytes());
        h.update(self.channel.name().as_bytes());
        let mut buf = [0; 32];
        h.finalize(&mut buf);
        hex::encode(&buf)
    }

    pub fn path(&self, node_config: &NodeConfigCached) -> PathBuf {
        let hash = self.hash();
        let hc = self.hash_channel();
        PathBuf::from(format!("{}", err::todoval::<u8>()))
            .join("cache")
            .join(&hc[0..3])
            .join(&hc[3..6])
            .join(self.channel.name())
            .join(format!("{}", self.agg_kind))
        /*.join(format!(
            "{:010}-{:010}",
            self.patch.spec().bin_t_len() / SEC,
            self.patch.spec().patch_t_len() / SEC
        ))
        .join(format!("{}-{:012}", &hash[0..6], self.patch.ix()))*/
    }
}

pub struct WrittenPbCache {
    pub bytes: u64,
    pub duration: Duration,
}

// TODO only used for old archiver
pub async fn write_pb_cache_min_max_avg_scalar<T>(
    values: T,
    patch: PreBinnedPatchCoordEnum,
    agg_kind: AggKind,
    channel: SfDbChannel,
    node_config: NodeConfigCached,
) -> Result<WrittenPbCache, Error>
where
    T: Serialize,
{
    let cfd = CacheFileDesc {
        channel: channel.clone(),
        patch: patch.clone(),
        agg_kind: agg_kind.clone(),
    };
    let path = cfd.path(&node_config);
    let enc = serde_cbor::to_vec(&values)?;
    let ts1 = Instant::now();
    tokio::fs::create_dir_all(path.parent().unwrap()).await.map_err(|e| {
        error!("can not create cache directory {:?}", path.parent());
        e
    })?;
    let now = Utc::now();
    let mut h = crc32fast::Hasher::new();
    h.update(&now.timestamp_nanos().to_le_bytes());
    let r = h.finalize();
    let tmp_path =
        path.parent()
            .unwrap()
            .join(format!("{}.tmp.{:08x}", path.file_name().unwrap().to_str().unwrap(), r));
    let res = tokio::task::spawn_blocking({
        let tmp_path = tmp_path.clone();
        move || {
            use fs2::FileExt;
            use io::Write;
            info!("try to write tmp at {:?}", tmp_path);
            let mut f = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp_path)?;
            if false {
                f.lock_exclusive()?;
            }
            f.write_all(&enc)?;
            if false {
                f.unlock()?;
            }
            f.flush()?;
            Ok::<_, Error>(enc.len())
        }
    })
    .await
    .map_err(Error::from_string)??;
    tokio::fs::rename(&tmp_path, &path).await?;
    let ts2 = Instant::now();
    let ret = WrittenPbCache {
        bytes: res as u64,
        duration: ts2.duration_since(ts1),
    };
    Ok(ret)
}

#[derive(Serialize)]
pub struct ClearCacheAllResult {
    pub log: Vec<String>,
}

pub async fn clear_cache_all(node_config: &NodeConfigCached, dry: bool) -> Result<ClearCacheAllResult, Error> {
    let mut log = vec![];
    log.push(format!("begin at {:?}", chrono::Utc::now()));
    if dry {
        log.push(format!("dry run"));
    }
    let mut dirs = VecDeque::new();
    let mut stack = VecDeque::new();
    // stack.push_front(node_config.node.cache_base_path.join("cache"));
    stack.push_front(PathBuf::from("UNDEFINED/NOTHING/REMOVE/ME").join("cache"));
    loop {
        match stack.pop_front() {
            Some(path) => {
                info!("clear_cache_all  try read dir {:?}", path);
                let mut rd = tokio::fs::read_dir(path).await?;
                while let Some(entry) = rd.next_entry().await? {
                    let path = entry.path();
                    match path.to_str() {
                        Some(_pathstr) => {
                            let meta = path.symlink_metadata()?;
                            //log.push(format!("len {:7}  pathstr {}", meta.len(), pathstr,));
                            let filename_str = path.file_name().unwrap().to_str().unwrap();
                            if filename_str.ends_with("..") || filename_str.ends_with(".") {
                                log.push(format!("ERROR encountered . or .."));
                            } else {
                                if meta.is_dir() {
                                    stack.push_front(path.clone());
                                    dirs.push_front((meta.len(), path));
                                } else if meta.is_file() {
                                    log.push(format!("remove file  len {:7}  {}", meta.len(), path.to_string_lossy()));
                                    if !dry {
                                        match tokio::fs::remove_file(&path).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                log.push(format!(
                                                    "can not remove file  {}  {:?}",
                                                    path.to_string_lossy(),
                                                    e
                                                ));
                                            }
                                        }
                                    }
                                } else {
                                    log.push(format!("not file, note dir"));
                                }
                            }
                        }
                        None => {
                            log.push(format!("Invalid utf-8 path encountered"));
                        }
                    }
                }
            }
            None => break,
        }
    }
    log.push(format!(
        "start to remove {} dirs at {:?}",
        dirs.len(),
        chrono::Utc::now()
    ));
    for (len, path) in dirs {
        log.push(format!("remove dir  len {}  {}", len, path.to_string_lossy()));
        if !dry {
            match tokio::fs::remove_dir(&path).await {
                Ok(_) => {}
                Err(e) => {
                    log.push(format!("can not remove dir  {}  {:?}", path.to_string_lossy(), e));
                }
            }
        }
    }
    log.push(format!("done at {:?}", chrono::Utc::now()));
    let ret = ClearCacheAllResult { log };
    Ok(ret)
}
