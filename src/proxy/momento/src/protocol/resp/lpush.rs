// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;

use crate::*;
use momento::cache::ListConcatenateFrontRequest;
use protocol_resp::{ListPush, LPUSH, LPUSH_EX};

use super::update_method_metrics;

pub async fn lpush(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListPush,
) -> ProxyResult {
    update_method_metrics(&LPUSH, &LPUSH_EX, async move {
        let r = ListConcatenateFrontRequest::new(
            cache_name,
            &*req.key(),
            req.elements().iter().map(|e| &e[..]),
        )
        .ttl(COLLECTION_TTL)
        .truncate_back_to_size(None);
        let _resp = timeout(Duration::from_millis(200), client.send_request(r)).await??;

        // Momento rust sdk doesn't return the count of elements added,
        // so we assume it's the same as the number of elements in the request.
        write!(response_buf, ":{}\r\n", req.elements().len())?;

        Ok(())
    })
    .await
}
