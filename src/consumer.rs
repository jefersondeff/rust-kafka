use kafka::consumer::{Consumer, FetchOffset, Message, MessageSet, MessageSets};
use serde_json::Value;
use std::str;

pub struct MyConsumer {
    consumer: Consumer,
}

impl MyConsumer {
    pub fn new(hosts: Vec<String>, topic: String) -> Self {
        Self {
            consumer: Consumer::from_hosts(hosts)
                .with_topic(topic)
                .with_fallback_offset(FetchOffset::Latest)
                .create()
                .unwrap(),
        }
    }

    pub fn get_event_data(m: &Message) -> Value {
        let event = str::from_utf8(m.value).unwrap().to_string();
        serde_json::from_str(&event).unwrap()
    }

    pub fn consume_events(&mut self) -> MessageSets {
        self.consumer.poll().unwrap()
    }

    pub fn consume_messagetset(&mut self, ms: MessageSet) {
        self.consumer.consume_messageset(ms).unwrap();
    }

    pub fn commit_consumed(&mut self) {
        self.consumer.commit_consumed().unwrap();
    }
}
