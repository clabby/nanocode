#![cfg(target_os = "linux")]

pub mod bubblewrap;
pub mod confine;
pub mod egress;

pub const SYSTEM: [&str; 7] = [
    "/usr",
    "/bin",
    "/sbin",
    "/lib",
    "/lib64",
    "/etc/ssl",
    "/etc/ca-certificates",
];

pub const PROXY_VARIABLES: [&str; 6] = [
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "ALL_PROXY",
    "https_proxy",
    "http_proxy",
    "all_proxy",
];
