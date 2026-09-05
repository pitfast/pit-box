fn main() {
    let mut memory = Vec::with_capacity(32 * 1024 * 1024);
    for _ in 0..(32 * 1024 * 1024) {
        memory.push(0xa5_u8);
    }
    println!("allocated {} bytes", memory.len());
}
