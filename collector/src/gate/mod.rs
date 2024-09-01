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
    if let Some(j_channel) = j.get("channel") {
        let channel = j_channel.as_str().ok_or(ConnectorError::FormatError)?;
        // let symbol = topic.split(".").last().ok_or(ConnectorError::FormatError)?;
        // let _ = writer_tx.send((recv_time, symbol.to_string(), data));
        
        if let Some(result_) = j.get("result") {
            if channel == "futures.order" {
                if let Some(contract_) = result_.get("contract") {
                    let symbol = contract_.as_str().unwrap();
                    // println!("{}", data);
                    let _ = writer_tx.send((recv_time, symbol.to_string(), data));
                    
                }
                
            }
            else if channel == "futures.trades" {
                let mut symbols = Vec::new();
                if let Some(results) = result_.as_array() {
                    for trade in results {
                        if let Some(contract) = trade.get("contract").and_then(|c| c.as_str()) {
                            if !symbols.contains(&contract.to_string()) {
                                symbols.push(contract.to_string());
                                break; // each futures.trades response only contains one symbol
                            }
                        }
                    }
                }
                
                for symbol in symbols {
                    let _ = writer_tx.send((recv_time, symbol, data.clone()));
                }
            }
            
            else if channel == "futures.order_book_update" {
                if let Some(contract_) = result_.get("s") {
                    let symbol = contract_.as_str().unwrap();
                    // println!("{}", data);
                    let _ = writer_tx.send((recv_time, symbol.to_string(), data));
                    
                }
                
            }
            else if channel == "futures.book_ticker" {
                if let Some(contract_) = result_.get("s") {
                    let symbol = contract_.as_str().unwrap();
                    // println!("{}", data);
                    let _ = writer_tx.send((recv_time, symbol.to_string(), data));
                    
                }
                
            }

        }
    } else if j.get("event").and_then(|v| v.as_str()) == Some("subscribe") {
        if let Some(result) = j.get("result") {
            if let Some(status) = result.get("status").and_then(|v| v.as_str()) {
                if status != "success" {
                    error!(%data, "couldn't subscribe the topics.");
                    return Err(ConnectorError::ConnectionAbort);
                }
            }
        }
    }
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