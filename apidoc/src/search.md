# Search Channels

To search for e.g. DBPM channels in `sf-databuffer` that end in `:Q1` the request
looks like this:

```bash
curl "https://data-api.psi.ch/api/4/search/channel?backend=sf-databuffer&nameRegex=DBPM.*Q1$"
```

Parameters:
- `icase=true` uses case-insensitive search (default: case-sensitive).
