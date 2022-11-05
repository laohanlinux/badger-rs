use async_trait::async_trait;
use std::io::{Read, Write};

use crate::Result;

pub trait Encode {
    fn enc(&self, wt: &mut dyn Write) -> Result<usize>;
}

pub trait Decode {
    fn dec(&mut self, rd: &mut dyn Read) -> Result<()>;
}

#[async_trait]
pub trait AsyncEncDec {
    async fn enc(&self, wt: &mut dyn Write) -> Result<usize>;
    async fn dec(&mut self, rd: &mut dyn Read) -> Result<()>;
}
