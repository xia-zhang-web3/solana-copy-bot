use super::*;

// Optional frozen-fixture regression layer. The capture-only reader and offline
// entrypoint do not call this function or require any of these original files.
pub(super) fn read(dir: &Path, original: &Path) -> reader::Capture {
    let capture = reader::read(dir).unwrap();
    let inputs: Value =
        serde_json::from_slice(&std::fs::read(original.join("inputs.json")).unwrap()).unwrap();
    let rows = capture.manifest["messages"].as_array().unwrap();
    assert_eq!(rows.len(), inputs.as_array().unwrap().len());
    for (i, row) in rows.iter().enumerate() {
        if row["saved"] != true {
            continue;
        }
        let file = format!("{:06}.pb", i + 1);
        let raw = std::fs::read(dir.join(&file)).unwrap();
        assert_eq!(
            raw,
            std::fs::read(original.join(file)).unwrap(),
            "no frozen input normalization"
        );
        let decoded = SubscribeUpdate::decode(raw.as_slice()).unwrap();
        let time = decoded
            .created_at
            .map(|t| json!({"seconds":t.seconds,"nanos":t.nanos}));
        assert_eq!(json!(time), inputs[i]["created_at"]);
    }
    capture
}
