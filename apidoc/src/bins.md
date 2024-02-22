# Binned Data

Binned data can be fetched like this:

```bash
curl "https://data-api.psi.ch/api/4/binned?backend=sf-databuffer&channelName=S10BC01-DBPM010:Q1&begDate=2024-02-15T00:00:00Z&endDate=2024-02-15T12:00:00Z&binCount=500"
```

This returns for each bin the average, minimum, maximum and count of events.

Note: the server may return more than `binCount` bins.
That is because most of the time, the requested combination of date range and bin count
does not fit well on the common time grid, which is required for caching to work.

If absolutely required, we could re-crunch the numbers to calculate the exact
requested specification of date range and bin count. Please get in touch
if your use case demands this.
