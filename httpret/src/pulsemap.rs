use crate::response;
use err::Error;
use http::{Method, StatusCode};
use hyper::{Body, Request, Response};
use netpod::NodeConfigCached;

pub struct MapPulseHisto {
    _pulse: u64,
    _tss: Vec<u64>,
    _counts: Vec<u64>,
}

const MAP_PULSE_HISTO_URL_PREFIX: &'static str = "/api/1/map/pulse/histo/";
const MAP_PULSE_URL_PREFIX: &'static str = "/api/1/map/pulse/";

fn _make_tables() -> Result<(), Error> {
    let _sql = "create table if not exists map_pulse_channels (name text, tbmax int)";
    let _sql = "create table if not exists map_pulse_files (channel text not null, split int not null, timebin int not null, closed int not null default 0, pulse_min int8 not null, pulse_max int8 not null)";
    let _sql = "create unique index if not exists map_pulse_files_ix1 on map_pulse_files (channel, split, timebin)";
    err::todoval()
}

pub struct MapPulseHistoHttpFunction {}

impl MapPulseHistoHttpFunction {
    pub fn path_matches(path: &str) -> bool {
        path.starts_with(MAP_PULSE_HISTO_URL_PREFIX)
    }

    pub fn handle(req: Request<Body>, _node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        let urls = format!("{}", req.uri());
        let _pulse: u64 = urls[MAP_PULSE_HISTO_URL_PREFIX.len()..].parse()?;
        Ok(response(StatusCode::NOT_IMPLEMENTED).body(Body::empty())?)
    }
}

pub struct MapPulseHttpFunction {}

impl MapPulseHttpFunction {
    pub fn path_matches(path: &str) -> bool {
        path.starts_with(MAP_PULSE_URL_PREFIX)
    }

    pub fn handle(req: Request<Body>, _node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        let urls = format!("{}", req.uri());
        let _pulse: u64 = urls[MAP_PULSE_URL_PREFIX.len()..].parse()?;
        Ok(response(StatusCode::NOT_IMPLEMENTED).body(Body::empty())?)
    }
}
