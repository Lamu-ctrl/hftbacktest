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
    topics: Vec<String>,
    pairs_str: String,
    ws_tx: UnboundedSender<(DateTime<Utc>, String)>,
) -> Result<(), anyhow::Error> {
    let request = url.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = unbounded_channel::<()>();

    println!("{}", format!(
        r#"{{"type": "subscribe", "instruments": {}, "channels": [{}],"interval": "raw"}}"#,
        pairs_str,
        topics
            .iter()
            .map(|s| format!("\"{s}\""))
            .collect::<Vec<_>>()
            .join(",")
    ));

    write
        .send(Message::Text(format!(
            r#"{{"type": "subscribe", "instruments": {}, "channels": [{}],"interval": "raw"}}"#,
            pairs_str,
            topics
                .iter()
                .map(|s| format!("\"{s}\""))
                .collect::<Vec<_>>()
                .join(",")
        )))
        .await?;

    tokio::spawn(async move {
        let mut ping_interval = tokio::time::interval(Duration::from_secs(10));
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
                        Message::Text(r#"{"type": "ping", "params":{"id":111}}"#.to_string())
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
            Some(Ok(Message::Ping(ping_payload))) => {
                // print the ping message
                // println!("Received Ping: {:?}", ping_payload);
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
                    .cloned()
                    .map(|stream| {
                        stream
                            .replace("$symbol", pair.to_uppercase().as_str())
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect::<Vec<_>>();

        let pairs_ = symbol_list
            .iter()
            .map(|s| {
                let upper = s.to_uppercase();
                format!("{}", &upper)
            })
            .collect::<Vec<_>>();
        let pairs_str = format!("[\"{}\"]", pairs_.join("\",\""));
        println!("{}", pairs_str);

        if let Err(error) = connect(
            "wss://ws.bit.com",
            topics_,
            pairs_str,
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
