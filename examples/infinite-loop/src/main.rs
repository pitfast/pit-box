use std::hint::black_box;

fn main() {
    let mut state = 0_u64;
    loop {
        state = state
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .wrapping_add(0x6a09_e667_f3bc_c909);
        state ^= state.rotate_left(13);
        black_box(state);
    }
}
