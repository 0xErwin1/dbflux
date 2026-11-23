use super::{decode_uri_component, split_uri_host_port, uri_host};

#[test]
fn split_uri_host_port_handles_names_and_ipv4() {
    assert_eq!(
        split_uri_host_port("db.local:5432"),
        ("db.local", Some("5432"))
    );
    assert_eq!(split_uri_host_port("10.0.0.1"), ("10.0.0.1", None));
    assert_eq!(split_uri_host_port(""), ("", None));
}

#[test]
fn split_uri_host_port_unbrackets_ipv6_hosts() {
    assert_eq!(split_uri_host_port("[::1]:5432"), ("::1", Some("5432")));
    assert_eq!(split_uri_host_port("[::1]"), ("::1", None));
    assert_eq!(
        split_uri_host_port("[2001:db8::7]:3306"),
        ("2001:db8::7", Some("3306"))
    );
}

#[test]
fn uri_host_brackets_only_ipv6_literals() {
    assert_eq!(uri_host("::1"), "[::1]");
    assert_eq!(uri_host("[::1]"), "[::1]");
    assert_eq!(uri_host("db.local"), "db.local");
    assert_eq!(uri_host("127.0.0.1"), "127.0.0.1");
}

#[test]
fn decode_uri_component_decodes_and_keeps_invalid_input() {
    assert_eq!(decode_uri_component("my%20db"), "my db");
    assert_eq!(decode_uri_component("plain"), "plain");
    assert_eq!(decode_uri_component("%FF"), "%FF");
}
