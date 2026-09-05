fn main() {
    println!("stdout before exit");
    eprintln!("stderr before exit");
    std::process::exit(7);
}
