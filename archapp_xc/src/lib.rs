use err::Error;
use serde::Serialize;

pub type ItemSerBox = Box<dyn ItemSer + Send>;

pub trait ItemSer {
    fn serialize(&self) -> Result<Vec<u8>, Error>;
}

impl<T> ItemSer for T
where
    T: Serialize,
{
    fn serialize(&self) -> Result<Vec<u8>, Error> {
        let u = serde_json::to_vec(self)?;
        Ok(u)
    }
}
