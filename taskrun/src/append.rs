use err::Error;
use std::borrow::Cow;
use std::fs;
use std::io::{BufWriter, Read, Seek, SeekFrom, Stdin, Write};
use std::path::{Path, PathBuf};

pub struct Buffer {
    buf: Vec<u8>,
    wp: usize,
    rp: usize,
}

const BUFFER_CAP: usize = 1024 * 8;

impl Buffer {
    pub fn new() -> Buffer {
        Self {
            buf: vec![0; BUFFER_CAP],
            wp: 0,
            rp: 0,
        }
    }

    pub fn reset(&mut self) {
        self.rp = 0;
        self.wp = 0;
    }

    pub fn len(&self) -> usize {
        self.wp - self.rp
    }

    pub fn check_invariant(&self) {
        if self.wp > self.buf.len() {
            eprintln!("ERROR  wp {}  rp {}", self.wp, self.rp);
        }
        if self.rp > self.wp {
            eprintln!("ERROR  wp {}  rp {}", self.wp, self.rp);
        }
        assert!(self.wp <= self.buf.len());
        assert!(self.rp <= self.wp);
    }

    pub fn writable(&mut self) -> &mut [u8] {
        self.check_invariant();
        self.wrap_if_needed();
        &mut self.buf[self.wp..]
    }

    pub fn readable(&self) -> &[u8] {
        self.check_invariant();
        &self.buf[self.rp..self.wp]
    }

    pub fn advance(&mut self, c: usize) {
        self.check_invariant();
        if c > self.len() {
            eprintln!("ERROR advance  wp {}  rp {}  c {}", self.wp, self.rp, c);
        }
        assert!(c <= self.len());
        self.rp += c;
    }

    pub fn inc_wp(&mut self, c: usize) {
        self.check_invariant();
        if c > self.buf.len() - self.wp {
            eprintln!("ERROR inc_wp  wp {}  rp {}  c {}", self.wp, self.rp, c);
        }
        assert!(c <= self.buf.len() - self.wp);
        self.wp += c;
    }

    fn wrap_if_needed(&mut self) {
        self.check_invariant();
        //eprintln!("wrap_if_needed  wp {}  rp {}", self.wp, self.rp);
        if self.wp == 0 {
        } else if self.rp == self.wp {
            self.rp = 0;
            self.wp = 0;
        } else if self.rp > self.buf.len() / 4 * 3 {
            if self.rp >= self.wp {
                eprintln!("ERROR wrap_if_needed  wp {}  rp {}", self.wp, self.rp);
            }
            assert!(self.rp < self.wp);
            let ll = self.len();
            unsafe {
                let src = &self.buf[self.rp..][0] as *const u8;
                let dst = &mut self.buf[..][0] as *mut u8;
                std::ptr::copy(src, dst, ll);
            }
            self.rp = 0;
            self.wp = ll;
        } else if self.wp == self.buf.len() {
            //eprintln!("ERROR no more space in buffer");
        }
    }
}

fn parse_lines(buf: &[u8]) -> Result<(Vec<Cow<str>>, usize), Error> {
    let mut ret = vec![];
    let mut i1 = 0;
    let mut i2 = 0;
    while i1 < buf.len() {
        if buf[i1] == 0xa {
            ret.push(String::from_utf8_lossy(&buf[i2..i1]));
            i1 += 1;
            i2 += 1;
        } else {
            break;
        }
    }
    while i1 < buf.len() {
        if buf[i1] == 0xa {
            const MAX: usize = 1024;
            if i2 + MAX < i1 {
                ret.push(String::from_utf8_lossy(&buf[i2..(i2 + MAX)]));
            } else {
                ret.push(String::from_utf8_lossy(&buf[i2..i1]));
            }
            i1 += 1;
            i2 = i1;
            while i1 < buf.len() {
                if buf[i1] == 0xa {
                    ret.push(String::from_utf8_lossy(&buf[i2..i1]));
                    i1 += 1;
                    i2 += 1;
                } else {
                    break;
                }
            }
        }
        i1 += 1;
    }
    Ok((ret, i2))
}

const MAX_PER_FILE: u64 = 1024 * 1024 * 2;
const MAX_TOTAL_SIZE: u64 = 1024 * 1024 * 20;

struct Fileinfo {
    path: PathBuf,
    name: String,
    len: u64,
}

fn file_list(dir: &Path) -> Result<Vec<Fileinfo>, Error> {
    let mut ret = vec![];
    let rd = fs::read_dir(&dir)?;
    for e in rd {
        let e = e?;
        let fnos = e.file_name();
        let fns = fnos.to_str().unwrap_or("");
        if fns.starts_with("info-20") && fns.ends_with(".log") {
            let meta = e.metadata()?;
            let info = Fileinfo {
                path: e.path(),
                name: fns.into(),
                len: meta.len(),
            };
            ret.push(info);
        }
    }
    ret.sort_by(|a, b| std::cmp::Ord::cmp(&a.name, &b.name));
    Ok(ret)
}

fn open_latest_or_new(dir: &Path) -> Result<BufWriter<fs::File>, Error> {
    let list = file_list(dir)?;
    if let Some(latest) = list.last() {
        if latest.len < MAX_PER_FILE {
            let ret = fs::OpenOptions::new().write(true).append(true).open(&latest.path)?;
            let ret = BufWriter::new(ret);
            return Ok(ret);
        }
    }
    next_file(dir)
}

fn next_file(dir: &Path) -> Result<BufWriter<fs::File>, Error> {
    let ts = chrono::Utc::now();
    let s = ts.format("%Y-%m-%d--%H-%M-%S").to_string();
    let mut ret = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(dir.join(format!("info-{}.log", s)))?;
    if ret.seek(SeekFrom::Current(0))? != 0 {
        return Err(Error::with_msg_no_trace("new file already exists"));
    }
    let ret = BufWriter::new(ret);
    Ok(ret)
}

pub fn append_inner(dirname: &str, mut stdin: Stdin) -> Result<(), Error> {
    let mut bytes_written = 0;
    let dir = PathBuf::from(dirname);
    let mut fout = open_latest_or_new(&dir)?;
    let mut buf = Buffer::new();
    loop {
        // Get some more data.
        let mut b = buf.writable();
        if false {
            write!(&mut fout, "[APPEND-WRITABLE]  {} writable bytes\n", b.len())?;
        }
        if b.len() == 0 {
            write!(&mut fout, "[DISCARD]  {} discarded bytes\n", b.len())?;
            buf.reset();
            b = buf.writable();
        }
        let b = b;
        if b.len() == 0 {
            let msg = format!("[ERROR DISCARD]  still no space  wp {}  rp {}\n", buf.wp, buf.rp);
            write!(&mut fout, "{}", msg)?;
            let e = Error::with_msg_no_trace(msg);
            return Err(e);
        }
        let n1 = stdin.read(b)?;
        buf.inc_wp(n1);
        if false {
            eprintln!(
                "{} bytes read from stdin, total readable {} bytes",
                n1,
                buf.readable().len()
            );
        }
        if false {
            write!(
                &mut fout,
                "[APPEND-INFO]  {} bytes read from stdin, total readable {} bytes\n",
                n1,
                buf.readable().len()
            )?;
        }
        match parse_lines(buf.readable()) {
            Ok((lines, n2)) => {
                if false {
                    eprintln!("parse_lines  Ok  n2 {n2}  lines len {}", lines.len());
                }
                if false {
                    write!(&mut fout, "[APPEND-PARSED-LINES]: {}\n", lines.len())?;
                }
                for line in lines {
                    let j = line.as_bytes();
                    fout.write_all(j)?;
                    fout.write_all(b"\n")?;
                    bytes_written += j.len() as u64 + 1;
                }
                buf.advance(n2);
                if buf.len() > 256 {
                    write!(&mut fout, "[TRUNCATED LINE FOLLOWS]\n")?;
                    fout.write_all(&buf.readable()[..256])?;
                    fout.write_all(b"\n")?;
                    buf.reset();
                }
            }
            Err(e) => {
                eprintln!("ERROR parse fail: {e}");
                write!(&mut fout, "[APPEND-PARSE-ERROR]: {e}\n")?;
                return Ok(());
            }
        }
        fout.flush()?;
        if bytes_written >= (MAX_PER_FILE >> 3) {
            bytes_written = 0;
            let l1 = fout.seek(SeekFrom::End(0))?;
            if l1 >= MAX_PER_FILE {
                let rd = fs::read_dir(&dir)?;
                let mut w = vec![];
                for e in rd {
                    let e = e?;
                    let fnos = e.file_name();
                    let fns = fnos.to_str().unwrap();
                    if fns.starts_with("info-20") && fns.ends_with(".log") {
                        let meta = e.metadata()?;
                        w.push((e.path(), meta.len()));
                    }
                }
                w.sort_by(|a, b| std::cmp::Ord::cmp(a, b));
                for q in &w {
                    write!(&mut fout, "[APPEND-SEES-FILE] {}\n", q.0.to_string_lossy())?;
                }
                let mut lentot = w.iter().map(|g| g.1).fold(0, |a, x| a + x);
                write!(&mut fout, "[APPEND-LENTOT] {}\n", lentot)?;
                for q in w {
                    if lentot <= MAX_TOTAL_SIZE as u64 {
                        break;
                    }
                    write!(&mut fout, "[APPEND-REMOVE]   {}  {}\n", q.1, q.0.to_string_lossy())?;
                    fs::remove_file(q.0)?;
                    if q.1 < lentot {
                        lentot -= q.1;
                    } else {
                        lentot = 0;
                    }
                }
                fout = next_file(&dir)?;
            };
        }
        if n1 == 0 {
            eprintln!("break because n1 == 0");
            break Ok(());
        }
    }
}

pub fn append(dirname: &str, stdin: Stdin) -> Result<(), Error> {
    match append_inner(dirname, stdin) {
        Ok(k) => {
            eprintln!("append_inner has returned");
            Ok(k)
        }
        Err(e) => {
            eprintln!("ERROR append {e:?}");
            let dir = PathBuf::from(dirname);
            let mut fout = open_latest_or_new(&dir)?;
            let _ = write!(fout, "ERROR in append_inner: {e:?}");
            Err(e)
        }
    }
}

#[test]
fn test_vec_index() {
    let mut buf = vec![0u8; BUFFER_CAP];
    let a = &mut buf[BUFFER_CAP - 1..BUFFER_CAP];
    a[0] = 123;
    let a = &mut buf[BUFFER_CAP..];
    assert_eq!(a.len(), 0);
}
