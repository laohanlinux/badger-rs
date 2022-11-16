use std::sync::Arc;
use std::sync::atomic::AtomicI32;
use parking_lot::RwLock;
use crate::kv::WeakKV;
use crate::table::builder;
use crate::table::table::TableCore;

pub(crate) struct LevelHandler {
    // Guards tables, total_size.
    tables: Arc<RwLock<(Vec<TableCore>, i64)>>,
    // The following are initialized once and const.
    level: Arc<i32>,
    str_level: Arc<String>,
    max_total_size: Arc<i64>,
    kv: WeakKV,
}

impl LevelHandler {
    fn init_tables(&self, tables: Vec<TableCore>) {
        let total_size = tables.iter().fold(0, |acc, &table| acc + table.size());
        let tb = self.tables.write();
        tb.0 = tables;
        tb.1 = total_size;
    }
}