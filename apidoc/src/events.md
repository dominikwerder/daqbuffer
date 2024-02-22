# Event Data

Event data can be fetched like this:

```bash
curl "https://data-api.psi.ch/api/4/events?backend=sf-databuffer&channelName=S10BC01-DBPM010:Q1&begDate=2024-02-15T12:41:00Z&endDate=2024-02-15T12:42:00Z"
```

Note: if the channel changes data type within the requested date range, then the
server will return values for that data type which covers the requested
date range best.

Parameters:
- `backend`: the backend that the channel exists in, e.g. `sf-databuffer`.
- `channelName`: the name of the channel.
- `begDate`: start of the time range, inclusive. In ISO format e.g. `2024-02-15T12:41:00Z`.
- `endDate`: end of the time range, exclusive.
- `allowLargeResult=true` indicates that the client is prepared to accept also larger responses compared to
  what might be suitable for a typical browser.
