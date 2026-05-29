use crate::types::StatusCode;

pub struct Diagnostic;

impl Diagnostic {
    pub fn log(code: StatusCode, message: &str) {
        println!("[YUUMI][{}] {}", code as u32, message);
    }

    pub fn error(code: StatusCode, details: &str) {
        eprintln!("[YUUMI_ERR][{}] {}", code as u32, details);
    }

    pub fn success(message: &str) {
        println!("[YUUMI][OK] {}", message);
    }
}
