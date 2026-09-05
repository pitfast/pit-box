use std::hint::black_box;

const DEFAULT_ITERATIONS: u64 = 50_000_000;

#[inline(never)]
fn cpu_burn(iterations: u64) -> u64 {
    let mut state = 0x243f_6a88_85a3_08d3_u64;
    let mut index = 0_u64;

    while index < iterations {
        state = state.wrapping_add(index.rotate_left(17));
        state ^= state.rotate_left(29);
        state = state
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .wrapping_add(0x6a09_e667_f3bc_c909);
        state ^= state >> 31;
        index += 1;
    }

    state
}

fn main() {
    let iterations = std::env::args()
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_ITERATIONS);
    let checksum = cpu_burn(iterations);

    // Keep the deterministic result observable to the optimizer without adding
    // one line of guest stdout for every concurrent benchmark invocation.
    black_box(checksum);
}
