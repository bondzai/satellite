use futures::{SinkExt, StreamExt};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    Message,
};
use tokio::sync::broadcast;
use warp::Filter;

#[tokio::main]
async fn main() {
    // Shared broadcast channel for forwarding messages.
    let (tx, _) = broadcast::channel::<String>(16);

    // Start the Kafka consumer task.
    tokio::spawn(consume_kafka(tx.clone()));

    // Define a WebSocket endpoint at "/ws".
    let ws_route = warp::path("ws")
        .and(warp::ws())
        .map(move |ws: warp::ws::Ws| {
            let rx = tx.subscribe();
            ws.on_upgrade(move |socket| handle_ws(socket, rx))
        });

    warp::serve(ws_route)
        .run(([127, 0, 0, 1], 3030))
        .await;
}

async fn consume_kafka(tx: broadcast::Sender<String>) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "group1")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Failed to create Kafka consumer");

    consumer.subscribe(&["my_topic"]).expect("Subscription failed");

    // Use the new stream() method instead of the deprecated start()
    let mut stream = consumer.stream();

    while let Some(result) = stream.next().await {
        if let Ok(msg) = result {
            // Match the nested Option<Result<_, _>> to extract a valid payload.
            if let Some(Ok(payload)) = msg.payload_view::<str>() {
                let _ = tx.send(payload.to_string());
            }
        }
    }
}

async fn handle_ws(ws: warp::ws::WebSocket, mut rx: broadcast::Receiver<String>) {
    let (mut ws_tx, _) = ws.split();
    while let Ok(msg) = rx.recv().await {
        if ws_tx.send(warp::ws::Message::text(msg)).await.is_err() {
            break;
        }
    }
}