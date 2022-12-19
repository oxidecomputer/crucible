// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    if version_check::is_min_version("1.59").unwrap_or(false) {
        println!("cargo:rustc-cfg=usdt_stable_asm");
    }

    #[cfg(target_os = "macos")]
    if version_check::is_min_version("1.66").unwrap_or(false) {
        println!("cargo:rustc-cfg=usdt_stable_asm_sym");
    }
}
