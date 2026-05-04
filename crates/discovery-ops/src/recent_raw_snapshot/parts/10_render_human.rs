fn render_human(output: &SnapshotOutput) -> String {
    let mut lines = Vec::new();
    append_render_overview(&mut lines, output);
    append_render_staged_progress(&mut lines, output);
    append_render_staged_write(&mut lines, output);
    append_render_snapshot_attempt(&mut lines, output);
    append_render_manifest(&mut lines, output);
    lines.join("\n")
}
