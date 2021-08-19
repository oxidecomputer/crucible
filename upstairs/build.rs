// Copyright 2021 Oxide Computer Company
#![feature(asm)]
use usdt::Builder;

fn main() {
    Builder::new("crutrace.d").build().unwrap();
}
