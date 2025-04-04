// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use pelikan_net::{TCP_ACCEPT, TCP_CLOSE, TCP_CONN_CURR};

pub(crate) async fn listener(
    listener: TcpListener,
    client_builder_config: MomentoClientBuilderConfig,
    cache_name: String,
    protocol: Protocol,
) {
    // this acts as our listener thread and spawns tasks for each client
    loop {
        // accept a new client
        if let Ok((socket, _)) = listener.accept().await {
            TCP_ACCEPT.increment();

            let client = match CacheClient::builder()
                .default_ttl(client_builder_config.default_ttl.clone())
                .configuration(client_builder_config.client_configuration.clone())
                .credential_provider(client_builder_config.credential_provider.clone())
                .build()
            {
                Ok(client) => client,
                Err(e) => {
                    error!(
                        "could not build cache client for cache `{}`: {}",
                        cache_name, e
                    );
                    TCP_CLOSE.increment();
                    continue;
                }
            };

            let cache_name = cache_name.clone();

            // spawn a task for managing requests for the client
            tokio::spawn(async move {
                TCP_CONN_CURR.increment();
                match protocol {
                    Protocol::Memcache => {
                        crate::frontend::handle_memcache_client(socket, client, cache_name).await;
                    }
                    Protocol::Resp => {
                        crate::frontend::handle_resp_client(socket, client, cache_name).await;
                    }
                }

                TCP_CONN_CURR.decrement();
                TCP_CLOSE.increment();
            });
        }
    }
}
