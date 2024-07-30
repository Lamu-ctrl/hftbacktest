use chrono::{DateTime, Utc};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::error;

use self::http::keep_connection;
use crate::error::ConnectorError;

mod http;

fn handle(
    writer_tx: &UnboundedSender<(DateTime<Utc>, String, String)>,
    recv_time: DateTime<Utc>,
    data: String,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(&data)?;
    // println!("j: {:?}", j);
    if let Some(j_topic) = j.get("channel") {
        let topic = j_topic.as_str().ok_or(ConnectorError::FormatError)?;
        if let Some(data_) = j.get("data") {
            if let Some(pair) = data_.get("instrument_id") {
                let symbol = pair.as_str().unwrap();
                let _ = writer_tx.send((recv_time, symbol.to_string(), data));
            }
        }
    }
    // // 先忽略訂閱成功所傳訊息
    // // https://www.bit.com/docs/zh-cn/spot.html#token
    // else if let Some(j_success) = j.get("success") {
    //     let success = j_success.as_bool().ok_or(ConnectorError::FormatError)?;
    //     if !success {
    //         error!(%data, "couldn't subscribe the topics.");
    //         return Err(ConnectorError::ConnectionAbort);
    //     }
    // }
    Ok(())
}

pub async fn run_collection(
    topics: Vec<String>,
    symbols: Vec<String>,
    writer_tx: UnboundedSender<(DateTime<Utc>, String, String)>,
) -> Result<(), anyhow::Error> {
    let (ws_tx, mut ws_rx) = unbounded_channel();
    let h = tokio::spawn(keep_connection(topics, symbols, ws_tx.clone()));
    loop {
        match ws_rx.recv().await {
            Some((recv_time, data)) => {
                if let Err(error) = handle(&writer_tx, recv_time, data) {
                    error!(?error, "couldn't handle the received data.");
                }
            }
            None => {
                break;
            }
        }
    }
    let _ = h.await;
    Ok(())
}
