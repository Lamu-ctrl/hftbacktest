use std::{
    io::{self, ErrorKind}, sync::mpsc::SendError, time::{Duration, Instant}
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
    symbol_list: Vec<String>,
    ws_tx: UnboundedSender<(DateTime<Utc>, String)>,
) -> Result<(), anyhow::Error> {
    let request = url.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = unbounded_channel::<()>();
    // ws.send('{"time" : 123456, "channel" : "futures.trades",
    //     "event": "subscribe", "payload" : ["BTC_USD"]}')
    // ws.send('{"time" : 123456, "channel" : "futures.order_book",
    // "event": "subscribe", "payload" : ["BTC_USD", "20", "0"]}')
    // ws.send('{"time" : 123456, "channel" : "futures.book_ticker",
    // "event": "subscribe", "payload" : ["BTC_USD"]}')
    // ws.send('{"time" : 123456, "channel" : "futures.order_book_update",
    // "event": "subscribe", "payload" : ["BTC_USD", "1000ms", "20"]}')

    for topic in topics {
        if topic == "futures.trades" {
            let current_timestamp = Utc::now().timestamp_millis();
            let payload = symbol_list.join("\",\"");

            write
                .send(Message::Text(format!(
                    r#"{{"time": {current_timestamp}, "channel": "futures.trades", "event": "subscribe", "payload": ["{payload}"]}}"#
                )))
                .await?;
        }
        else if topic == "futures.order_book" {
            for symbol in symbol_list.clone() {
                let current_timestamp = Utc::now().timestamp_millis();
                write
                    .send(Message::Text(format!(
                        r#"{{"time": {current_timestamp}, "channel": "futures.order_book", "event": "subscribe", "payload": ["{symbol}","100","0"]}}"#
                    )))
                    .await?;
            }
        }
        else if topic == "futures.book_ticker" {
            for symbol in symbol_list.clone() {
                let current_timestamp = Utc::now().timestamp_millis();
                write
                    .send(Message::Text(format!(
                        r#"{{"time": {current_timestamp}, "channel": "futures.book_ticker", "event": "subscribe", "payload": ["{symbol}"]}}"#
                    )))
                    .await?;
            }
        }
        else if topic == "futures.order_book_update" {
            for symbol in symbol_list.clone() {
                let current_timestamp = Utc::now().timestamp_millis();
                write
                    .send(Message::Text(format!(
                        r#"{{"time": {current_timestamp}, "channel": "futures.order_book_update", "event": "subscribe", "payload": ["{symbol}","20ms","20"]}}"#
                    )))
                    .await?;
            }
        }
    }
    // write
    //     .send(Message::Text(format!(
    //         r#"{{"req_id": "subscribe", "event": "subscribe", "channel": [{}]}}"#,
    //         topics
    //             .iter()
    //             .map(|s| format!("\"{s}\""))
    //             .collect::<Vec<_>>()
    //             .join(",")
    //     )))
    //     .await?;

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
                    let current_timestamp = Utc::now().timestamp_millis();
                    if let Err(_) = write.send(
                        Message::Text(format!(
                            r#"{{"time": {current_timestamp}, "channel" : "futures.ping"}}"#
                        ))
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
        // let topics_ = symbol_list
        //     .iter()
        //     .map(|pair| {
        //         topics
        //             .iter()
        //             .cloned()
        //             .map(|stream| {
        //                 stream
        //                     .replace("$symbol", pair.to_uppercase().as_str())
        //                     .to_string()
        //             })
        //             .collect::<Vec<_>>()
        //     })
        //     .flatten()
        //     .collect::<Vec<_>>();
        if let Err(error) = connect(
            "wss://fx-ws.gateio.ws/v4/ws/usdt",
            topics.clone(),
            symbol_list.clone(),
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
