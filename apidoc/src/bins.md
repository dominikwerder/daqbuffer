# Binned Data

Binned data can be fetched like this:

```bash
curl "https://data-api.psi.ch/api/4/binned?backend=sf-databuffer&channelName=S10BC01-DBPM010:Q1&begDate=2024-02-15T00:00:00Z&endDate=2024-02-15T12:00:00Z&binWidth="
```

Parameters:
- `backend`: the backend that the channel exists in, e.g. `sf-databuffer`.
- `channelName`: the name of the channel.
- `begDate`: start of the time range, inclusive. In ISO format e.g. `2024-02-15T12:41:00Z`.
- `endDate`: end of the time range, exclusive.
- `binWidth`: requested width of the bins, given with a unit suffix e.g. `10s` for 10 seconds, `2m` for 2 minutes,
  `1h` for 1 hour.
- `binCount`: requested number of bins, can not be combined with `binWidth`.
- `contentTimeout`: return the so-far computed results after the given timeout.
  The streaming (e.g. `json-framed`) response will yield results in `contentTimeout` intervals.
- `allowLargeResult=true` **DEPRECATED, will be rejected in the future**
  indicates that the client is prepared to accept also larger responses compared to
  what might be suitable for a typical browser. Please download large result sets as
  framed json or framed cbor streams, see below.

This returns for each bin the average, minimum, maximum and count of events.

Note: it is an error to specify both `binWidth` and `binCount`.
The server may return more than `binCount` bins, and it will choose a `binWidth` from a set of
supported widths to best possibly match the requested width.


Example response, truncated to 4 bins:
```json
{
  "avgs": [
    0.010802877135574818,
    0.010565019212663174,
    0.01061472948640585,
    0.010656529106199741
  ],
  "counts": [
    3000,
    2999,
    3000,
    3000
  ],
  "maxs": [
    0.017492100596427917,
    0.016716860234737396,
    0.01769270747900009,
    0.01670699194073677
  ],
  "mins": [
    0.005227561108767986,
    0.0040797283872962,
    0.004329073242843151,
    0.004934651777148247
  ],
  "ts1Ms": [
    0,
    300000,
    600000,
    900000
  ],
  "ts1Ns": [
    0,
    0,
    0,
    0
  ],
  "ts2Ms": [
    300000,
    600000,
    900000,
    1200000
  ],
  "ts2Ns": [
    0,
    0,
    0,
    0
  ],
  "tsAnchor": 1726394700
}
```


## As framed JSON stream

To download larger amounts data as JSON it is recommended to use the `json-framed` content encoding.
Using this encoding, the server can send the requested events as a stream of json objects, where each
json object contains a batch of events.
This content encoding is triggered via the `Accept: application/json-framed` header in the request.

The returned body looks like:
```
[JSON-frame]
[JSON-frame]
[JSON-frame]
... etc
```

where each `[JSON-frame]` looks like:
```
[number of bytes N of the following json-encoded data, as ASCII-encoded number]
[newline]
[JSON object: N bytes]
[newline]
```

Note: "data" objects are currently identified by the presence of the `ts1s` key.
There can be other types of objects, like keepalive, log or statistics.
