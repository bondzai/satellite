use futures::{SinkExt, StreamExt};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    Message,
};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;
use warp::Filter;

/// Runs the Kafka consumer and WebSocket server.
/// Incoming Kafka messages are sent via a broadcast channel to connected WebSocket clients.
async fn run_server() {
    // Create a broadcast channel to share messages with WebSocket clients.
    let (tx, _) = broadcast::channel::<String>(16);

    // Spawn the Kafka consumer task.
    let consumer_tx = tx.clone();
    tokio::spawn(async move {
        consume_kafka(consumer_tx).await;
    });

    // Define a WebSocket endpoint at "/ws".
    let ws_route = warp::path("ws")
        .and(warp::ws())
        .map(move |ws: warp::ws::Ws| {
            let rx = tx.subscribe();
            ws.on_upgrade(move |socket| handle_ws(socket, rx))
        });

    println!("Starting WebSocket server on 127.0.0.1:3030/ws");
    warp::serve(ws_route).run(([127, 0, 0, 1], 3030)).await;
}

/// Runs the Kafka producer, simulating messages being sent to Kafka.
async fn run_client() {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Failed to create Kafka producer");

    let topic = "my_topic";
    let mut counter = 0;

    loop {
        let payload = format!("Message number {}", counter);
        let key = format!("key-{}", counter);
        let record = FutureRecord::to(topic)
            .payload(&payload)
            .key(&key);

        // Send the record and await the delivery status.
        match producer.send(record, Duration::from_secs(0)).await {
            Ok(delivery) => println!("Delivered: {:?}", delivery),
            Err((error, _)) => println!("Error delivering message: {:?}", error),
        }

        counter += 1;
        sleep(Duration::from_secs(2)).await;
    }
}

/// Kafka consumer that listens to messages and broadcasts them.
async fn consume_kafka(tx: broadcast::Sender<String>) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "group1")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Failed to create Kafka consumer");

    consumer.subscribe(&["my_topic"]).expect("Subscription failed");

    // Use the new stream() method.
    let mut stream = consumer.stream();

    while let Some(result) = stream.next().await {
        if let Ok(msg) = result {
            // Extract payload if it exists and is valid UTF-8.
            if let Some(Ok(payload)) = msg.payload_view::<str>() {
                let _ = tx.send(payload.to_string());
            }
        }
    }
}

/// Handles a single WebSocket connection, forwarding messages from the broadcast channel.
async fn handle_ws(ws: warp::ws::WebSocket, mut rx: broadcast::Receiver<String>) {
    let (mut ws_tx, _) = ws.split();
    while let Ok(msg) = rx.recv().await {
        if ws_tx.send(warp::ws::Message::text(msg)).await.is_err() {
            break;
        }
    }
}

#[tokio::main]
async fn main() {
    // Run both server and client concurrently.
    let server_handle = tokio::spawn(run_server());
    let client_handle = tokio::spawn(run_client());

    // Wait for both tasks (this runs indefinitely).
    let _ = tokio::join!(server_handle, client_handle);
}