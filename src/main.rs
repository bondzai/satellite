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

const KAFKA_BOOTSTRAP_SERVERS: &str = "localhost:9092";
const DEFAULT_TOPIC: &str = "default_topic";
const WS_SERVER_ADDR: ([u8; 4], u16) = ([127, 0, 0, 1], 3030);

fn create_producer() -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .create()
        .expect("Failed to create Kafka producer")
}

fn create_consumer() -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .set("group.id", "group1")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Failed to create Kafka consumer")
}

async fn run_server() {
    let (tx, _) = broadcast::channel::<String>(16);

    let consumer_tx = tx.clone();
    tokio::spawn(async move {
        consume_kafka(consumer_tx).await;
    });

    let ws_route = warp::path("ws")
        .and(warp::ws())
        .map(move |ws: warp::ws::Ws| {
            let rx = tx.subscribe();
            ws.on_upgrade(move |socket| handle_ws(socket, rx))
        });

    println!("Starting WebSocket server on {}:{} (endpoint: /ws)", WS_SERVER_ADDR.0.iter().map(|b| b.to_string()).collect::<Vec<_>>().join("."), WS_SERVER_ADDR.1);
    warp::serve(ws_route).run(WS_SERVER_ADDR).await;
}

async fn run_client() {
    let producer = create_producer();
    let mut counter = 0;

    loop {
        let payload = format!("Message number {}", counter);
        let key = format!("key-{}", counter);
        let record = FutureRecord::to(DEFAULT_TOPIC)
            .payload(&payload)
            .key(&key);

        match producer.send(record, Duration::from_secs(0)).await {
            Ok(delivery) => println!("Delivered: {:?}", delivery),
            Err((error, _)) => println!("Error delivering message: {:?}", error),
        }

        counter += 1;
        sleep(Duration::from_secs(2)).await;
    }
}

async fn consume_kafka(tx: broadcast::Sender<String>) {
    let consumer = create_consumer();
    consumer.subscribe(&[DEFAULT_TOPIC]).expect("Subscription failed");

    let mut stream = consumer.stream();
    while let Some(result) = stream.next().await {
        if let Ok(msg) = result {
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

#[tokio::main]
async fn main() {
    let server_handle = tokio::spawn(run_server());
    let client_handle = tokio::spawn(run_client());
    
    let _ = tokio::join!(server_handle, client_handle);
}