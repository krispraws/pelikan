// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::{cache::SetRemoveElementsRequest, CacheClient};
use protocol_resp::{SetRem, SREM, SREM_EX};

use crate::error::ProxyResult;

use super::update_method_metrics;

pub async fn srem(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SetRem,
) -> ProxyResult {
    update_method_metrics(&SREM, &SREM_EX, async move {
        let elements: Vec<&[u8]> = req.members().iter().map(|e| &**e).collect();

        let r = SetRemoveElementsRequest::new(cache_name, &*req.key(), elements);
        let _resp =
            tokio::time::timeout(Duration::from_millis(200), client.send_request(r)).await??;

        // Momento doesn't return the info we need here so we pretend that
        // all the elements were removed from the set.
        write!(response_buf, ":{}\r\n", req.members().len())?;

        Ok(())
    })
    .await
}
