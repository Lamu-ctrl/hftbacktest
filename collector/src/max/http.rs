use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    select,
    sync::mpsc::{unbounded_channel, UnboundedSender},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tracing::{error, warn};

pub async fn connect(
    url: &str,
    topics_str: String,
    ws_tx: UnboundedSender<(DateTime<Utc>, String)>,
) -> Result<(), anyhow::Error> {
    let request = url.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = unbounded_channel::<()>();
    // https://maicoin.github.io/max-websocket-docs/#/public_channels?id=subscribe
    // {
    //     "action": "sub",
    //     "subscriptions": [
    //       {"channel": "book", "market": "btctwd", "depth": 1},
    //       {"channel": "trade", "market": "btctwd"}
    //     ],
    //     "id": "client1"
    //   }
    println!("{}",format!(
        r#"{{"action": "sub",  "subscriptions": [{}] , "id": "toriii"}}"#,
        topics_str
    ));
    write
        .send(Message::Text(format!(
            r#"{{"action": "sub",  "subscriptions": [{}] , "id": "toriii"}}"#,
            topics_str
        )))
        .await?;

    tokio::spawn(async move {
        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            select! {
                result = rx.recv() => {
                    match result {
                        Some(_) => {
                            if let Err(_) = write.send(Message::Pong(Vec::new())).await {
                                return;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
                _ = ping_interval.tick() => {
                    if let Err(_) = write.send(
                        Message::Text(r#"{"req_id": "ping", "op": "ping"}"#.to_string())
                    ).await {
                        return;
                    }
                }
            }
        }
    });

    loop {
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                let recv_time = Utc::now();
                if let Err(_) = ws_tx.send((recv_time, text)) {
                    break;
                }
            }
            Some(Ok(Message::Binary(_))) => {}
            Some(Ok(Message::Ping(_))) => {
                tx.send(()).unwrap();
            }
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Close(close_frame))) => {
                warn!(?close_frame, "closed");
                return Err(Error::from(io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "closed",
                )));
            }
            Some(Ok(Message::Frame(_))) => {}
            Some(Err(e)) => {
                return Err(Error::from(e));
            }
            None => {
                break;
            }
        }
    }
    Ok(())
}

pub async fn keep_connection(
    topics: Vec<String>,
    symbol_list: Vec<String>,
    ws_tx: UnboundedSender<(DateTime<Utc>, String)>,
) {
    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
        let topics_ = symbol_list
        .iter()
        .map(|pair| {
            topics
                .iter()
                .map(|topic| {
                    format!("{{\"channel\": \"{}\", \"market\": \"{}\"}}", topic, pair)
                })
                .collect::<Vec<_>>()
        })
        .flatten()
        .collect::<Vec<_>>()
        .join(",");

        if let Err(error) = connect(
            "wss://max-stream.maicoin.com/ws",
            topics_,
            ws_tx.clone(),
        )
        .await
        {
            error!(?error, "websocket error");
            error_count += 1;
            if connect_time.elapsed() > Duration::from_secs(30) {
                error_count = 0;
            }
            if error_count > 3 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else if error_count > 10 {
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else if error_count > 20 {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        } else {
            break;
        }
    }
}
