// Copyright 2023 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::time::Duration;
use std::{collections::HashSet, io::Write};

use momento::{cache::SetFetchResponse, CacheClient};
use protocol_resp::{SetDiff, SDIFF, SDIFF_EX};
use tokio::time;

use crate::ProxyResult;

use super::update_method_metrics;

pub async fn sdiff(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &SetDiff,
) -> ProxyResult {
    update_method_metrics(&SDIFF, &SDIFF_EX, async move {
        let timeout = Duration::from_millis(200);

        // Note: the resp parser validates that SetDiff has at least one key.
        let (head, rest) = req
            .keys()
            .split_first()
            .expect("got an invalid set difference request");
        let head = &**head;

        let response = time::timeout(timeout, client.set_fetch(cache_name, head)).await??;
        let mut set = match response {
            SetFetchResponse::Hit { values } => {
                let elements: Vec<Vec<u8>> = values
                    .try_into()
                    .expect("Expected to fetch a list of byte vectors!");
                HashSet::<Vec<u8>>::from_iter(elements)
            }
            SetFetchResponse::Miss => HashSet::default(),
        };
        if set.is_empty() {
            response_buf.extend_from_slice(b"*0\r\n");
            return Ok(());
        };

        for key in rest {
            let key = &**key;

            if set.is_empty() {
                break;
            }

            let response = time::timeout(timeout, client.set_fetch(cache_name, key)).await??;
            match response {
                SetFetchResponse::Hit { values } => {
                    let elements: Vec<Vec<u8>> = values
                        .try_into()
                        .expect("Expected to fetch a list of byte vectors!");
                    for entry in elements {
                        set.remove(&entry);
                    }
                }
                SetFetchResponse::Miss => {
                    continue;
                }
            }
        }

        write!(response_buf, "*{}\r\n", set.len())?;

        for entry in &set {
            write!(response_buf, "${}\r\n", entry.len())?;
            response_buf.extend_from_slice(entry);
        }

        Ok(())
    })
    .await
}
