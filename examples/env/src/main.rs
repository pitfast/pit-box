fn main() {
    for key in ["MODE", "REGION"] {
        println!("{key}={}", std::env::var(key).unwrap_or_else(|_| "<unset>".to_owned()));
    }
}
