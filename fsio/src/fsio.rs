use err::Error;
use items::plainevents::PlainEvents;
use netpod::log::*;
use netpod::Channel;
#[allow(unused)]
use std::os::unix::prelude::OpenOptionsExt;
use std::os::unix::prelude::{AsRawFd, OsStrExt};
use std::path::PathBuf;
use tokio::fs::OpenOptions;

const BASE: &str = "/data/daqbuffer-testdata";

fn fcntl_xlock(file: &mut std::fs::File, beg: i64, cmd: libc::c_int, ty: i32) -> i32 {
    unsafe {
        let p = libc::flock {
            l_type: ty as i16,
            l_whence: libc::SEEK_SET as i16,
            l_start: beg,
            l_len: 8,
            l_pid: 0,
        };
        libc::fcntl(file.as_raw_fd(), cmd, &p)
    }
}

fn wlock(file: &mut std::fs::File, beg: i64) -> i32 {
    fcntl_xlock(file, beg, libc::F_OFD_SETLK, libc::F_WRLCK)
}

fn rlock(file: &mut std::fs::File, beg: i64) -> i32 {
    fcntl_xlock(file, beg, libc::F_OFD_SETLK, libc::F_RDLCK)
}

fn unlock(file: &mut std::fs::File, beg: i64) -> i32 {
    fcntl_xlock(file, beg, libc::F_OFD_SETLK, libc::F_UNLCK)
}

#[allow(unused)]
async fn lock_1() -> Result<(), Error> {
    let path = PathBuf::from(BASE).join("tmp-daq4-f1");
    let mut f1 = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .truncate(false)
        .open(path)
        .await?;
    f1.as_raw_fd();

    let mx1 = std::sync::Arc::new(tokio::sync::Mutex::new(0usize));
    let mg1 = mx1.lock().await;

    let (tx1, rx2) = std::sync::mpsc::channel();
    let (tx2, rx1) = std::sync::mpsc::channel();

    let t1 = std::thread::spawn({
        move || {
            let path = PathBuf::from(BASE).join("tmp-daq4-f1");
            let mut f1 = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
            info!("Thread 1 rlock...");
            let ec = rlock(&mut f1, 0);
            info!("Thread 1 rlock {}", ec);
            tx1.send(1u32).unwrap();
            rx1.recv().unwrap();
            info!("Thread 1 unlock...");
            let ec = unlock(&mut f1, 0);
            info!("Thread 1 unlock {}", ec);
            tx1.send(1u32).unwrap();
            rx1.recv().unwrap();
            info!("Thread 1 rlock...");
            let ec = rlock(&mut f1, 0);
            info!("Thread 1 rlock {}", ec);
            tx1.send(1u32).unwrap();
            rx1.recv().unwrap();
            info!("Thread 1 done");
        }
    });
    let t2 = std::thread::spawn({
        move || {
            let path = PathBuf::from(BASE).join("tmp-daq4-f1");
            let mut f1 = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
            rx2.recv().unwrap();
            info!("Thread 2 wlock...");
            let ec = wlock(&mut f1, 0);
            info!("Thread 2 wlock {}", ec);
            tx2.send(1u32).unwrap();
            rx2.recv().unwrap();
            info!("Thread 2 rlock");
            let ec = rlock(&mut f1, 0);
            info!("Thread 2 rlock {}", ec);
            tx2.send(1u32).unwrap();
            rx2.recv().unwrap();
            tx2.send(1u32).unwrap();
            info!("Thread 2 done");
        }
    });
    tokio::task::spawn_blocking(move || {
        t1.join().map_err(|_| Error::with_msg_no_trace("join error"))?;
        t2.join().map_err(|_| Error::with_msg_no_trace("join error"))?;
        Ok::<_, Error>(())
    })
    .await??;
    Ok(())
}

#[allow(unused)]
async fn write_1() -> Result<(), Error> {
    let path = PathBuf::from(BASE).join("tmp-daq4-f2");
    let mut f1 = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .truncate(false)
        .open(path)
        .await?;
    unsafe {
        let path_d = PathBuf::from(BASE);
        let mut path_d_b = path_d.as_os_str().as_bytes().to_vec();
        //info!("path_d_b {:?}", path_d_b);
        path_d_b.push(0);
        let fdd = libc::open(path_d_b.as_ptr() as *const i8, libc::O_DIRECTORY | libc::O_RDONLY);
        if fdd < 0 {
            panic!();
        }
        let ec = libc::fsync(fdd);
        if ec != 0 {
            panic!();
        }
        let ec = libc::close(fdd);
        if ec != 0 {
            panic!();
        }
        let fd = f1.as_raw_fd();
        let lockparam = libc::flock {
            l_type: libc::F_RDLCK as i16,
            l_whence: libc::SEEK_SET as i16,
            l_start: 0,
            l_len: 8,
            l_pid: 0,
        };
        let ec = libc::fcntl(f1.as_raw_fd(), libc::F_OFD_SETLK, &lockparam);
        if ec != 0 {
            panic!();
        }
        let buf = b"world!";
        let n = libc::pwrite(fd, buf.as_ptr() as *const libc::c_void, buf.len(), 0);
        if n != buf.len() as isize {
            panic!();
        }
        let ec = libc::fsync(fd);
        if ec != 0 {
            panic!();
        }
        let lockparam = libc::flock {
            l_type: libc::F_UNLCK as i16,
            l_whence: libc::SEEK_SET as i16,
            l_start: 0,
            l_len: 8,
            l_pid: 0,
        };
        let ec = libc::fcntl(f1.as_raw_fd(), libc::F_OFD_SETLK, &lockparam);
        if ec == 0 {
            panic!();
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(unused)]
mod test {
    use super::*;

    //#[test]
    fn t1() -> Result<(), Error> {
        Ok(taskrun::run(write_1()).unwrap())
    }
}

pub struct EventSink {}

impl EventSink {
    pub fn sink(&self, _channel: &Channel, _events: PlainEvents) -> Result<(), Error> {
        Ok(())
    }
}
