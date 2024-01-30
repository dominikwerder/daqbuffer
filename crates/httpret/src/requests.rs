use httpclient::http::header;
use httpclient::http::header::HeaderMap;
use netpod::ACCEPT_ALL;
use netpod::APP_CBOR_FRAMES;
use netpod::APP_JSON;

pub fn accepts_json_or_all(headers: &HeaderMap) -> bool {
    let accept_def = APP_JSON;
    let accept = headers
        .get(header::ACCEPT)
        .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
    accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL)
}

pub fn accepts_cbor_frames(headers: &HeaderMap) -> bool {
    let accept_def = "";
    let accept = headers
        .get(header::ACCEPT)
        .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
    accept.contains(APP_CBOR_FRAMES)
}
