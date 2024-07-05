use serde_json::json;

pub struct Texs {
    texts: Vec<String>,
}

impl Texs {
    pub fn new() -> Self {
        Self { texts: vec![] }
    }

    pub fn add_text(&mut self, text: String) {
        self.texts.push(text);
    }

    pub fn remove_text(&mut self, index: usize) {
        self.texts.remove(index);
    }

    pub fn to_json(&self) -> String {
        json!(&self.texts).to_string()
    }
}
