use err::Error;
use std::borrow::Cow;
use std::fs;
use std::io::{self, BufWriter, Read, Stderr, Stdin, Write};
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

    pub fn len(&self) -> usize {
        self.wp - self.rp
    }

    pub fn writable(&mut self) -> &mut [u8] {
        self.wrap_if_needed();
        &mut self.buf[self.wp..]
    }

    pub fn readable(&self) -> &[u8] {
        &self.buf[self.rp..self.wp]
    }

    pub fn advance(&mut self, c: usize) {
        assert!(c <= self.len());
        self.rp += c;
    }

    pub fn inc_wp(&mut self, c: usize) {
        assert!(c <= self.buf.len() - self.wp);
        self.wp += c;
    }

    fn wrap_if_needed(&mut self) {
        if self.rp == self.wp && self.rp != 0 {
            self.rp = 0;
            self.wp = 0;
        } else if self.rp > BUFFER_CAP / 4 * 3 {
            assert!(self.wp < BUFFER_CAP);
            assert!(self.rp <= self.wp);
            unsafe {
                let src = &self.buf[self.rp..][0] as *const u8;
                let dst = &mut self.buf[..][0] as *mut u8;
                std::ptr::copy(src, dst, self.len());
            }
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

const MAX_PER_FILE: usize = 1024 * 1024 * 2;
const MAX_TOTAL_SIZE: usize = 1024 * 1024 * 20;

fn next_file(dir: &Path, append: bool, truncate: bool) -> io::Result<BufWriter<fs::File>> {
    let ts = chrono::Utc::now();
    let s = ts.format("%Y-%m-%d--%H-%M-%S").to_string();
    let ret = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .append(append)
        .truncate(truncate)
        .open(dir.join(format!("info-{}.log", s)))?;
    let ret = BufWriter::new(ret);
    Ok(ret)
}

pub fn append_inner(dirname: &str, mut stdin: Stdin, _stderr: Stderr) -> Result<(), Error> {
    let mut bytes_written = 0;
    let dir = PathBuf::from(dirname);
    let mut fout = next_file(&dir, true, false)?;
    let mut buf = Buffer::new();
    loop {
        // Get some more data.
        let b = buf.writable();
        if false {
            write!(&mut fout, "{} writable bytes\n", b.len())?;
        }
        let n1 = stdin.read(b)?;
        buf.inc_wp(n1);
        if false {
            write!(
                &mut fout,
                "{} bytes read from stdin, total readable {} bytes\n",
                n1,
                buf.readable().len()
            )?;
        }
        match parse_lines(buf.readable()) {
            Ok((lines, n2)) => {
                if false {
                    write!(&mut fout, "parsed {} lines\n", lines.len())?;
                }
                for line in lines {
                    let j = line.as_bytes();
                    fout.write_all(j)?;
                    fout.write_all(b"\n")?;
                    bytes_written += j.len() + 1;
                }
                buf.advance(n2);
            }
            Err(e) => {
                write!(&mut fout, "parse error: {:?}\n", e)?;
                return Ok(());
            }
        }
        fout.flush()?;
        if bytes_written >= MAX_PER_FILE {
            bytes_written = 0;
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
                write!(&mut fout, "file:::: {}\n", q.0.to_string_lossy())?;
            }
            let mut lentot = w.iter().map(|g| g.1).fold(0, |a, x| a + x);
            write!(&mut fout, "lentot: {}\n", lentot)?;
            for q in w {
                if lentot <= MAX_TOTAL_SIZE as u64 {
                    break;
                }
                write!(&mut fout, "REMOVE   {}  {}\n", q.1, q.0.to_string_lossy())?;
                fs::remove_file(q.0)?;
                if q.1 < lentot {
                    lentot -= q.1;
                } else {
                    lentot = 0;
                }
            }
            fout = next_file(&dir, true, false)?;
        }
    }
}

pub fn append(dirname: &str, stdin: Stdin, _stderr: Stderr) -> Result<(), Error> {
    match append_inner(dirname, stdin, _stderr) {
        Ok(k) => Ok(k),
        Err(e) => {
            eprintln!("got error {:?}", e);
            Err(e)
        }
    }
}
