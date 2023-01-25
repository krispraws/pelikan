// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::klog::{klog_1, Status};
use crate::{Error, *};
use ::net::*;
use protocol_memcache::*;

pub async fn get(
    client: &mut SimpleCacheClient,
    cache_name: &str,
    socket: &mut tokio::net::TcpStream,
    key: &[u8],
) -> Result<(), Error> {
    GET.increment();

    // check if any of the key is invalid
    if std::str::from_utf8(key).is_err() {
        GET_EX.increment();

        // invalid key
        let _ = socket.write_all(b"-ERR invalid key\r\n").await;
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    let mut response_buf = Vec::new();

    BACKEND_REQUEST.increment();
    GET_KEY.increment();

    // we've already checked the keys, so we
    // know this unwrap is safe
    let key = std::str::from_utf8(key).unwrap();

    match timeout(Duration::from_millis(200), client.get(cache_name, key)).await {
        Ok(Ok(response)) => {
            match response.result {
                MomentoGetStatus::ERROR => {
                    // we got some error from
                    // the backend.
                    BACKEND_EX.increment();
                    GET_EX.increment();
                    response_buf.extend_from_slice(b"-ERR backend error\r\n");
                }
                MomentoGetStatus::HIT => {
                    GET_KEY_HIT.increment();

                    let item_header = format!("${}\r\n", response.value.len());

                    response_buf.extend_from_slice(item_header.as_bytes());
                    response_buf.extend_from_slice(&response.value);
                    response_buf.extend_from_slice(b"\r\n");

                    klog_1(&"get", &key, Status::Hit, response.value.len());
                }
                MomentoGetStatus::MISS => {
                    GET_KEY_MISS.increment();

                    response_buf.extend_from_slice(b"$-1\r\n");

                    klog_1(&"get", &key, Status::Miss, 0);
                }
            }
        }
        Ok(Err(MomentoError::LimitExceeded(_))) => {
            BACKEND_EX.increment();
            BACKEND_EX_RATE_LIMITED.increment();
            GET_EX.increment();
            response_buf.extend_from_slice(b"-ERR ratelimit exceed\r\n");
        }
        Ok(Err(e)) => {
            // we got some error from the momento client
            // log and incr stats and move on treating it
            // as an error
            error!("error for get: {}", e);
            BACKEND_EX.increment();
            GET_EX.increment();
            response_buf.extend_from_slice(b"-ERR backend error\r\n");
        }
        Err(_) => {
            // we had a timeout, incr stats and move on
            // treating it as an error
            BACKEND_EX.increment();
            BACKEND_EX_TIMEOUT.increment();
            GET_EX.increment();
            response_buf.extend_from_slice(b"-ERR backend timeout\r\n");
        }
    }

    SESSION_SEND.increment();
    SESSION_SEND_BYTE.add(response_buf.len() as _);
    TCP_SEND_BYTE.add(response_buf.len() as _);
    if let Err(e) = socket.write_all(&response_buf).await {
        SESSION_SEND_EX.increment();
        return Err(e);
    }
    Ok(())
}
