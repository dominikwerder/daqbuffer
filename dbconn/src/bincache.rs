use err::Error;
use scylla::Session as ScySession;

pub async fn search_channel_scylla<BINC>(_scy: &ScySession) -> Result<(), Error>
where
    BINC: Clone,
{
    todo!()
}
