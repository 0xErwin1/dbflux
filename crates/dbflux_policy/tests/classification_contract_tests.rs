use dbflux_policy::ExecutionClassification;

#[test]
fn classification_serde_values_are_stable() {
    let cases = vec![
        (ExecutionClassification::Metadata, "\"metadata\""),
        (ExecutionClassification::Read, "\"read\""),
        (ExecutionClassification::Write, "\"write\""),
        (ExecutionClassification::Destructive, "\"destructive\""),
        (ExecutionClassification::Admin, "\"admin\""),
    ];

    for (classification, expected) in cases {
        let json = serde_json::to_string(&classification).expect("serialization should work");
        assert_eq!(json, expected);

        let restored: ExecutionClassification =
            serde_json::from_str(&json).expect("deserialization should work");
        assert_eq!(restored, classification);
    }
}
