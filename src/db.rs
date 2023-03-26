use crate::kv::KV;
use crate::options::Options;
use crate::types::{XArc, XWeak};

pub struct DataBase {
    kv: XArc<KV>,
    VL: Option<VL>,
}

impl DataBase {
    async fn new() {
        let kv = KV::open(Options::default()).await;
    }
}

pub struct VL {
    kv: XWeak<KV>,
}
