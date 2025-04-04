// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::{
    cache::{ListFetchRequest, ListFetchResponse},
    CacheClient,
};
use protocol_resp::{ListRange, LRANGE, LRANGE_EX};
use tokio::time::timeout;

use crate::error::ProxyResult;

use super::update_method_metrics;

pub async fn lrange(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListRange,
) -> ProxyResult {
    update_method_metrics(&LRANGE, &LRANGE_EX, async move {
        let r = ListFetchRequest::new(cache_name, req.key())
            .start_index(i32::try_from(req.start()).ok())
            .end_index(i32::try_from(req.stop().saturating_add(1)).ok()); // Momento uses exclusive ends

        match timeout(Duration::from_millis(200), client.send_request(r)).await?? {
            ListFetchResponse::Hit { values } => {
                let elements: Vec<Vec<u8>> = values
                    .try_into()
                    .expect("Expected to fetch a list of byte vectors!");
                write!(response_buf, "*{}\r\n", elements.len())?;

                for elem in elements {
                    write!(response_buf, "${}\r\n", elem.len())?;
                    response_buf.extend_from_slice(&elem);
                }
            }
            ListFetchResponse::Miss => {
                response_buf.extend_from_slice(b"*0\r\n");
            }
        };
        Ok(())
    })
    .await
}
