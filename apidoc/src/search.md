# Search Channels

To search for e.g. DBPM channels in `sf-databuffer` that end in `:Q1` the request
looks like this:

```bash
curl "https://data-api.psi.ch/api/4/search/channel?backend=sf-archiver&nameRegex=S10CB08-KBOC-HP.*PI-OUT"
```

Parameters:
- `icase=true` uses case-insensitive search (default: case-sensitive).


Example response:
```json
{
  "channels": [
    {
      "backend": "sf-archiver",
      "name": "S10CB08-KBOC-HPPI1:PI-OUT",
      "seriesId": 6173407188734815544,
      "source": "",
      "type": "f64",
      "shape": [],
      "unit": "",
      "description": ""
    }
  ]
}
```

The response contains a list of matching channels.
For each channel, the search returns:

- `backend`
- `name`
- `seriesId`: a 63 bit id which identifies the combination of name, type and shape
  in a unique way within a backend. Therefore, if the type of a channel changes, it will
  also generate an additional new `seriesId`.
- `type`: the scalar data type, for example `i16`, `u64`, `f32`, `enum`, `string`, `bool`.
- `shape`: a `[]` means scalar, a `[512]` indicates a 512 element waveform.

Note that "source", "unit" and "description" are in the return value for historical reasons.
Unfortunately, the data is often not available, and in general may also change over time.
Therefore, "source", "unit" and "description" are deprecated and will get removed.
