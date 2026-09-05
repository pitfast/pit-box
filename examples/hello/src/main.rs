fn main() {
    println!("Hello from PitFast!");
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !args.is_empty() {
        println!("Arguments: {}", args.join(" | "));
    }
}
