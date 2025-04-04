// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Write;
use std::time::Duration;

use momento::cache::{ListFetchRequest, ListFetchResponse};
use momento::CacheClient;
use protocol_resp::{ListIndex, LINDEX, LINDEX_EX, LINDEX_HIT, LINDEX_MISS};

use crate::error::ProxyResult;
use crate::klog::{klog_2, Status};
use crate::ProxyError;

use super::update_method_metrics;

pub async fn lindex(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &ListIndex,
) -> ProxyResult {
    update_method_metrics(&LINDEX, &LINDEX_EX, async move {
        let r = ListFetchRequest::new(cache_name, req.key())
            .start_index(i32::try_from(req.index()).ok())
            .end_index(i32::try_from(req.index().saturating_add(1)).ok()); // Momento uses exclusive ends
        match tokio::time::timeout(Duration::from_millis(200), client.send_request(r)).await {
            Ok(Ok(r)) => {
                let status = match r {
                    ListFetchResponse::Hit { values } => {
                        let elements: Vec<Vec<u8>> = values
                            .try_into()
                            .expect("Expected to fetch a list of byte vectors!");
                        if elements.len() != 1 {
                            return Err(ProxyError::custom("Unexpected response from server"));
                        } else {
                            write!(response_buf, "${}\r\n", elements[0].len())?;
                            response_buf.extend_from_slice(&elements[0]);
                            response_buf.extend_from_slice(b"\r\n");

                            LINDEX_HIT.increment();
                            Status::Hit
                        }
                    }
                    ListFetchResponse::Miss => {
                        write!(response_buf, "$-1\r\n")?;

                        LINDEX_MISS.increment();
                        Status::Miss
                    }
                };

                let index = format!("{}", req.index());
                klog_2(&"lindex", &req.key(), &index, status, response_buf.len());
                Ok(())
            }
            Ok(Err(e)) => {
                let index = format!("{}", req.index());
                klog_2(&"lindex", &req.key(), &index, Status::ServerError, 0);

                Err(ProxyError::from(e))
            }
            Err(e) => {
                let index = format!("{}", req.index());
                klog_2(&"lindex", &req.key(), &index, Status::Timeout, 0);

                Err(ProxyError::from(e))
            }
        }
    })
    .await
}
