use log::{info, kv::source::as_map, kv::Source, Level};
use rand::random;
use std::collections::HashMap;
use std::env::temp_dir;
use std::fs::create_dir_all;

#[cfg(test)]
pub(crate) fn mock_log() {
    use chrono::Local;
    use env_logger::Env;
    use log::kv::source::AsMap;
    use log::kv::{Error, Key, ToKey, ToValue, Value};
    use serde::{Deserialize, Serialize};
    use std::io::Write;

    #[derive(Serialize, Deserialize)]
    struct JsonLog {
        level: log::Level,
        ts: String,
        module: String,
        msg: String,
        #[serde(skip_serializing_if = "HashMap::is_empty", flatten)]
        kv: HashMap<String, serde_json::Value>,
    }

    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", "error")
        .write_style_or("MY_LOG_STYLE", "always");
    let _ = env_logger::Builder::from_env(env)
        .format(|buf, record| {
            let mut l = JsonLog {
                ts: Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
                module: record.file().unwrap_or("unknown").to_string()
                    + ":"
                    + &*record.line().unwrap_or(0).to_string(),
                level: record.level(),
                msg: record.args().to_string(),
                kv: Default::default(),
            };
            let kv: AsMap<&dyn Source> = as_map(record.key_values());
            if let Ok(kv) = serde_json::to_string(&kv) {
                let h: HashMap<String, serde_json::Value> = serde_json::from_str(&kv).unwrap();
                l.kv.extend(h.into_iter());
            }
            writeln!(buf, "{}", serde_json::to_string(&l).unwrap())
        })
        .try_init();
    log::info!( is_ok = true; "start init log");
    // env_logger::try_init_from_env(env);
}

#[cfg(test)]
pub(crate) fn mock_log_terminal() {
    console_log::init_with_level(Level::Debug);
}

pub fn random_tmp_dir() -> String {
    let id = random::<u32>();
    let path = temp_dir().join(id.to_string()).join("badger");
    // create_dir_all(&path).unwrap();
    path.to_str().unwrap().to_string()
}
