use kafka::consumer::{Consumer, FetchOffset};
use std::str;
///Create consumer
fn main() {
    let hosts = vec!["localhost:29092".to_owned()];
    let mut consumer = match Consumer::from_hosts(hosts)
        .with_topic("topic-name".to_owned())
        .with_fallback_offset(FetchOffset::Latest)
        .create()
    {
        Ok(consumer) => consumer,
        Err(e) => {
            eprintln!("Erro ao criar consumer: {}", e);
            return;
        }
    };

    loop {
        match consumer.poll() {
            Ok(message_sets) => {
                for ms in message_sets.iter() {
                    for m in ms.messages() {
                        println!("{:?}", str::from_utf8(m.value).unwrap());
                    }
                    if let Err(e) = consumer.consume_messageset(ms) {
                        println!("Falied consumed mensagen set: {}", e)
                    };
                }
                if let Err(e) = consumer.commit_consumed() {
                    println!("Failed to commit consumed messages: {}", e)
                };
            }
            Err(e) => {
                eprintln!("Failed to poll messages: {}", e)
            }
        }
    }
}
