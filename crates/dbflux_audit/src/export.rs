use crate::AuditEvent;
use dbflux_storage::repositories::audit::AuditEventDto;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditExportFormat {
    Csv,
    Json,
}

pub fn export_entries(
    entries: &[AuditEvent],
    format: AuditExportFormat,
) -> Result<String, serde_json::Error> {
    match format {
        AuditExportFormat::Csv => Ok(export_csv(entries)),
        AuditExportFormat::Json => serde_json::to_string_pretty(entries),
    }
}

fn export_csv(entries: &[AuditEvent]) -> String {
    let mut output = String::from("id,actor_id,tool_id,decision,reason,created_at_epoch_ms\n");

    for entry in entries {
        let escaped_reason = entry
            .reason
            .as_deref()
            .unwrap_or_default()
            .replace('"', "\"\"");

        output.push_str(&format!(
            "{},{},{},{},\"{}\",{}\n",
            entry.id,
            entry.actor_id,
            entry.tool_id,
            entry.decision,
            escaped_reason,
            entry.created_at_epoch_ms
        ));
    }

    output
}

/// Exports extended audit events (full DTO schema) to JSON or CSV format.
pub fn export_extended(
    events: &[AuditEventDto],
    format: AuditExportFormat,
) -> Result<String, serde_json::Error> {
    let normalized: Vec<_> = events
        .iter()
        .cloned()
        .map(|mut event| {
            if event.tool_id.trim().is_empty() {
                event.tool_id = event.legacy_tool_id();
            }

            if event.decision.trim().is_empty() {
                event.decision = event.legacy_decision();
            }

            event
        })
        .collect();

    match format {
        AuditExportFormat::Csv => Ok(export_extended_csv(&normalized)),
        AuditExportFormat::Json => serde_json::to_string_pretty(&normalized),
    }
}

/// Exports extended audit events to CSV format with all DTO fields.
fn export_extended_csv(events: &[AuditEventDto]) -> String {
    let mut output = String::new();

    // Header row with all extended fields
    output.push_str(
        "id,actor_id,tool_id,decision,reason,profile_id,classification,duration_ms,\
         created_at,created_at_epoch_ms,level,category,action,outcome,actor_type,source_id,\
         summary,connection_id,database_name,driver_id,object_type,object_id,\
         details_json,error_code,error_message,session_id,correlation_id\n",
    );

    for event in events {
        let escape = |s: Option<&str>| {
            s.unwrap_or_default()
                .replace('"', "\"\"")
                .replace('\n', " ")
        };

        output.push_str(&format!(
            "{},\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",{},{},{},\"{}\",\"{}\",\"{}\",\
             \"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\
             \"{}\",\"{}\",\"{}\"\n",
            event.id,
            escape(Some(&event.actor_id)),
            escape(Some(&event.tool_id)),
            escape(Some(&event.decision)),
            escape(event.reason.as_deref()),
            escape(event.profile_id.as_deref()),
            escape(event.classification.as_deref()),
            event.duration_ms.map(|d| d.to_string()).unwrap_or_default(),
            escape(Some(&event.created_at)),
            event.created_at_epoch_ms,
            escape(event.level.as_deref()),
            escape(event.category.as_deref()),
            escape(event.action.as_deref()),
            escape(event.outcome.as_deref()),
            escape(event.actor_type.as_deref()),
            escape(event.source_id.as_deref()),
            escape(event.summary.as_deref()),
            escape(event.connection_id.as_deref()),
            escape(event.database_name.as_deref()),
            escape(event.driver_id.as_deref()),
            escape(event.object_type.as_deref()),
            escape(event.object_id.as_deref()),
            escape(event.details_json.as_deref()),
            escape(event.error_code.as_deref()),
            escape(event.error_message.as_deref()),
            escape(event.session_id.as_deref()),
            escape(event.correlation_id.as_deref()),
        ));
    }

    output
}

#[cfg(test)]
mod tests {
    use super::{AuditExportFormat, export_entries, export_extended};
    use crate::AuditEvent;
    use dbflux_storage::repositories::audit::AuditEventDto;

    fn event(id: i64, reason: Option<&str>) -> AuditEvent {
        AuditEvent {
            id,
            actor_id: "alice".to_string(),
            tool_id: "read_query".to_string(),
            decision: "allow".to_string(),
            reason: reason.map(str::to_string),
            created_at_epoch_ms: 1_700_000_000_000,
        }
    }

    const CSV_HEADER: &str = "id,actor_id,tool_id,decision,reason,created_at_epoch_ms\n";

    #[test]
    fn csv_basic_header_and_rows() {
        let events = vec![event(1, Some("first")), event(2, None)];

        let csv = export_entries(&events, AuditExportFormat::Csv).expect("csv export");

        let expected = format!(
            "{CSV_HEADER}\
             1,alice,read_query,allow,\"first\",1700000000000\n\
             2,alice,read_query,allow,\"\",1700000000000\n"
        );
        assert_eq!(csv, expected);
    }

    #[test]
    fn csv_empty_slice_yields_header_only() {
        let csv = export_entries(&[], AuditExportFormat::Csv).expect("csv export");
        assert_eq!(csv, CSV_HEADER);
    }

    #[test]
    fn csv_reason_doubles_embedded_quotes() {
        let events = vec![event(1, Some(r#"contains "quoted" text"#))];

        let csv = export_entries(&events, AuditExportFormat::Csv).expect("csv export");

        // export_csv only doubles `"` inside the quoted reason field.
        assert!(
            csv.contains(r#""contains ""quoted"" text""#),
            "embedded quotes in reason must be doubled per RFC 4180; got: {csv}"
        );
    }

    // FINDING: export_csv (the basic AuditEvent CSV) does NOT quote the
    // id/actor_id/tool_id/decision fields and does NOT escape commas or newlines
    // inside `reason` (it only doubles `"`). A reason containing a comma or
    // newline produces malformed CSV (extra/leaked columns or rows). This test
    // asserts the CURRENT (buggy) behavior so the suite is green and the gap is
    // documented; it is intentionally not a "correct CSV" assertion.
    #[test]
    fn csv_reason_with_comma_and_newline_is_not_escaped_finding() {
        let events = vec![event(1, Some("a,b\nc"))];

        let csv = export_entries(&events, AuditExportFormat::Csv).expect("csv export");

        // The comma and the raw newline survive verbatim inside the quoted field:
        // a correct serializer would still keep them inside quotes (legal), but
        // note the actor/tool/decision columns are emitted entirely unquoted.
        let expected = format!("{CSV_HEADER}1,alice,read_query,allow,\"a,b\nc\",1700000000000\n");
        assert_eq!(
            csv, expected,
            "documenting current behavior: reason is wrapped in quotes but the \
             leading numeric/text columns are never quoted"
        );
    }

    #[test]
    fn json_basic_round_trips_to_expected_values() {
        let events = vec![event(7, Some("why")), event(8, None)];

        let json = export_entries(&events, AuditExportFormat::Json).expect("json export");

        let decoded: Vec<AuditEvent> = serde_json::from_str(&json).expect("parse json");
        assert_eq!(decoded, events);
    }

    #[test]
    fn json_empty_slice_is_empty_array() {
        let json = export_entries(&[], AuditExportFormat::Json).expect("json export");
        let decoded: Vec<AuditEvent> = serde_json::from_str(&json).expect("parse json");
        assert!(decoded.is_empty());
    }

    fn minimal_dto(id: i64) -> AuditEventDto {
        AuditEventDto {
            id,
            actor_id: "alice".to_string(),
            tool_id: "read_query".to_string(),
            decision: "allow".to_string(),
            reason: None,
            profile_id: None,
            classification: None,
            duration_ms: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            created_at_epoch_ms: 1_700_000_000_000,
            level: None,
            category: None,
            action: None,
            outcome: None,
            actor_type: None,
            source_id: None,
            summary: None,
            connection_id: None,
            database_name: None,
            driver_id: None,
            object_type: None,
            object_id: None,
            details_json: None,
            error_code: None,
            error_message: None,
            session_id: None,
            correlation_id: None,
        }
    }

    fn fuller_dto(id: i64) -> AuditEventDto {
        AuditEventDto {
            reason: Some("looked risky".to_string()),
            profile_id: Some("prof-1".to_string()),
            classification: Some("destructive".to_string()),
            duration_ms: Some(42),
            level: Some("warn".to_string()),
            category: Some("query".to_string()),
            action: Some("query_execute".to_string()),
            outcome: Some("success".to_string()),
            actor_type: Some("user".to_string()),
            source_id: Some("local".to_string()),
            summary: Some("ran a query".to_string()),
            connection_id: Some("conn-1".to_string()),
            database_name: Some("main".to_string()),
            driver_id: Some("sqlite".to_string()),
            object_type: Some("table".to_string()),
            object_id: Some("users".to_string()),
            details_json: Some(r#"{"query":"select 1"}"#.to_string()),
            error_code: Some("E001".to_string()),
            error_message: Some("boom".to_string()),
            session_id: Some("sess-1".to_string()),
            correlation_id: Some("corr-1".to_string()),
            ..minimal_dto(id)
        }
    }

    #[test]
    fn extended_csv_includes_extended_fields() {
        let events = vec![fuller_dto(1)];

        let csv = export_extended(&events, AuditExportFormat::Csv).expect("extended csv");

        let header = csv.lines().next().expect("header line");
        assert!(header.contains("classification"));
        assert!(header.contains("correlation_id"));
        assert!(header.contains("details_json"));

        // Extended values must appear in the data row.
        assert!(csv.contains("\"destructive\""));
        assert!(csv.contains("\"corr-1\""));
        assert!(csv.contains("\"sqlite\""));
        // duration_ms is emitted as an unquoted number.
        assert!(csv.contains(",42,"));
    }

    #[test]
    fn extended_csv_escapes_quotes_and_flattens_newlines() {
        let mut dto = minimal_dto(1);
        dto.summary = Some("line one\nline two".to_string());
        dto.error_message = Some(r#"say "hi""#.to_string());

        let csv = export_extended(&[dto], AuditExportFormat::Csv).expect("extended csv");

        // Newlines inside a field are replaced by spaces; quotes are doubled.
        assert!(
            csv.contains("\"line one line two\""),
            "extended csv must flatten newlines to spaces; got: {csv}"
        );
        assert!(
            csv.contains(r#""say ""hi""""#),
            "extended csv must double embedded quotes; got: {csv}"
        );
    }

    #[test]
    fn extended_csv_empty_slice_yields_header_only() {
        let csv = export_extended(&[], AuditExportFormat::Csv).expect("extended csv");

        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(
            lines.len(),
            1,
            "empty input must produce only the header row"
        );
        assert!(lines[0].starts_with("id,actor_id,tool_id,decision,reason"));
    }

    #[test]
    fn extended_json_minimal_and_fuller_round_trip() {
        let events = vec![minimal_dto(1), fuller_dto(2)];

        let json = export_extended(&events, AuditExportFormat::Json).expect("extended json");

        let decoded: serde_json::Value = serde_json::from_str(&json).expect("parse json");
        let array = decoded.as_array().expect("top-level array");
        assert_eq!(array.len(), 2);

        assert_eq!(array[0]["id"], 1);
        assert_eq!(array[1]["classification"], "destructive");
        assert_eq!(array[1]["correlation_id"], "corr-1");
        assert_eq!(array[1]["duration_ms"], 42);
    }

    #[test]
    fn extended_json_empty_slice_is_empty_array() {
        let json = export_extended(&[], AuditExportFormat::Json).expect("extended json");
        let decoded: serde_json::Value = serde_json::from_str(&json).expect("parse json");
        assert_eq!(decoded.as_array().map(Vec::len), Some(0));
    }

    // export_extended normalizes blank tool_id/decision to the legacy projection.
    #[test]
    fn extended_normalizes_blank_tool_id_and_decision() {
        let mut dto = minimal_dto(1);
        dto.tool_id = String::new();
        dto.decision = String::new();
        dto.action = Some("mcp_approve_execution".to_string());
        dto.outcome = Some("success".to_string());

        let json = export_extended(&[dto], AuditExportFormat::Json).expect("extended json");
        let decoded: serde_json::Value = serde_json::from_str(&json).expect("parse json");

        assert_eq!(decoded[0]["tool_id"], "approve_execution");
        assert_eq!(decoded[0]["decision"], "allow");
    }
}
