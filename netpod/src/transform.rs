use crate::get_url_query_pairs;
use crate::AppendToUrl;
use crate::FromUrl;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transform {
    array_pick: Option<usize>,
}

impl Transform {
    fn url_prefix() -> &'static str {
        "transform"
    }
}

impl FromUrl for Transform {
    fn from_url(url: &url::Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &std::collections::BTreeMap<String, String>) -> Result<Self, err::Error> {
        let upre = Self::url_prefix();
        let ret = Self {
            array_pick: pairs
                .get(&format!("{}ArrayPick", upre))
                .map(|x| match x.parse::<usize>() {
                    Ok(n) => Some(n),
                    Err(_) => None,
                })
                .unwrap_or(None),
        };
        Ok(ret)
    }
}

impl AppendToUrl for Transform {
    fn append_to_url(&self, url: &mut url::Url) {
        let upre = Self::url_prefix();
        let mut g = url.query_pairs_mut();
        if let Some(x) = &self.array_pick {
            g.append_pair(&format!("{}ArrayPick", upre), &format!("{}", x));
        }
    }
}
