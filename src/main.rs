use consumer::MyConsumer;
use producer::MyProducer;
use text::Texs;

mod consumer;
mod producer;
mod text;

fn main() {
    let hosts = vec!["localhost:9092".to_string()];
    let mut texts = Texs::new();
    let mut consumer = MyConsumer::new(hosts.clone(), "actions".to_string());
    let mut producer = MyProducer::new(hosts);

    println!("Started...");

    loop {
        for ms in consumer.consume_events().iter() {
            for m in ms.messages() {
                let event_data = MyConsumer::get_event_data(m);
                let action = event_data["action"].to_string();
                if action == "\"add\"" {
                    texts.add_text(event_data["value"].to_string());
                } else if action == "\"remove\"" {
                    let index = event_data["value"].to_string().parse::<usize>().unwrap();
                    texts.remove_text(index)
                } else {
                    println!("Invalid action");
                }
                producer.send_data_to_topic("texts", texts.to_json());
            }
            consumer.consume_messagetset(ms);
        }
        consumer.commit_consumed();
    }
}
