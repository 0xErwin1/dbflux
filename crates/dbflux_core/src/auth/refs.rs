//! Auth profile reference expansion.
//!
//! Some auth providers declare fields of kind `FormFieldKind::AuthProfileRef`
//! that point at another `AuthProfile` (e.g. an `aws-sso` profile referencing
//! an `aws-sso-session` profile that owns the shared `sso_start_url`).
//!
//! `expand_auth_profile_refs` walks the provider's form definition and, for
//! each `AuthProfileRef` field whose value is set to a known profile UUID,
//! copies the referenced profile's fields into the consumer profile's field
//! map. Existing non-empty values on the consumer profile are preserved, so a
//! profile can override individual fields inline if needed.

use std::collections::HashMap;

use uuid::Uuid;

use crate::auth::AuthFormDef;
use crate::auth::types::AuthProfile;
use crate::driver::form::FormFieldKind;

/// Lookup function: resolves a profile UUID to the persisted `AuthProfile`.
///
/// The implementation typically captures a snapshot of the auth profile
/// registry. Returning `None` means the reference is dangling and that
/// `AuthProfileRef` field is left unexpanded.
pub type AuthProfileLookup<'a> = dyn Fn(&Uuid) -> Option<AuthProfile> + 'a;

/// Returns a clone of `profile` with all `AuthProfileRef` fields expanded.
///
/// For each field in `form_def` whose kind is `AuthProfileRef`, this function:
/// 1. Reads the consumer profile's field value (expected to be a UUID).
/// 2. Calls `lookup` with that UUID.
/// 3. Merges the referenced profile's `fields` into the result — but only for
///    keys whose value on the consumer is empty or missing. Inline overrides
///    on the consumer always win.
///
/// The reference field itself (e.g. `sso_session_ref`) is left in place so
/// the UI can still display which session was selected.
///
/// If a referenced profile cannot be resolved, that reference is silently
/// skipped. Other fields are still expanded.
pub fn expand_auth_profile_refs(
    profile: &AuthProfile,
    form_def: &AuthFormDef,
    lookup: &AuthProfileLookup<'_>,
) -> AuthProfile {
    let mut expanded = profile.clone();

    for tab in &form_def.tabs {
        for section in &tab.sections {
            for field in &section.fields {
                let FormFieldKind::AuthProfileRef { .. } = &field.kind else {
                    continue;
                };

                let Some(raw) = expanded.fields.get(&field.id).map(String::as_str) else {
                    continue;
                };

                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let Ok(target_id) = Uuid::parse_str(trimmed) else {
                    continue;
                };

                let Some(referenced) = lookup(&target_id) else {
                    continue;
                };

                // Stash the referenced profile's display name under
                // `<field_id>_name` so downstream providers can refer to the
                // target by its human-readable name (e.g. an `aws-sso`
                // provider writing back an `[profile X] sso_session = NAME`
                // block needs the session's name, not its UUID).
                let name_field = format!("{}_name", field.id);
                expanded.fields.insert(name_field, referenced.name.clone());

                merge_referenced_fields(&mut expanded.fields, &referenced.fields);
            }
        }
    }

    expanded
}

fn merge_referenced_fields(
    consumer: &mut HashMap<String, String>,
    referenced: &HashMap<String, String>,
) {
    for (key, value) in referenced {
        let consumer_value = consumer.get(key).map(String::as_str).unwrap_or("").trim();
        if consumer_value.is_empty() {
            consumer.insert(key.clone(), value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::driver::form::{FormFieldDef, FormSection, FormTab};

    fn form_with_ref_field(consumer_field_id: &str, ref_provider_id: &str) -> AuthFormDef {
        AuthFormDef {
            tabs: vec![FormTab {
                id: "main".to_string(),
                label: "Main".to_string(),
                sections: vec![FormSection {
                    title: "Session".to_string(),
                    fields: vec![FormFieldDef {
                        id: consumer_field_id.to_string(),
                        label: "Session".to_string(),
                        kind: FormFieldKind::AuthProfileRef {
                            provider_id: ref_provider_id.to_string(),
                        },
                        placeholder: String::new(),
                        required: false,
                        default_value: String::new(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                        disabled_when_field_set: None,
                        help: None,
                    }],
                }],
            }],
        }
    }

    fn profile_with(provider_id: &str, entries: &[(&str, &str)]) -> AuthProfile {
        let mut fields = HashMap::new();
        for (key, value) in entries {
            fields.insert((*key).to_string(), (*value).to_string());
        }
        AuthProfile::new("test", provider_id, fields)
    }

    #[test]
    fn ref_field_merges_missing_keys() {
        let referenced = profile_with(
            "ref-provider",
            &[
                ("sso_start_url", "https://example.awsapps.com/start/"),
                ("sso_region", "us-east-1"),
            ],
        );
        let referenced_id = referenced.id;

        let consumer = profile_with(
            "consumer-provider",
            &[("session_ref", &referenced_id.to_string())],
        );

        let form_def = form_with_ref_field("session_ref", "ref-provider");

        let expanded = expand_auth_profile_refs(&consumer, &form_def, &|id| {
            if *id == referenced_id {
                Some(referenced.clone())
            } else {
                None
            }
        });

        assert_eq!(
            expanded.fields.get("sso_start_url").map(String::as_str),
            Some("https://example.awsapps.com/start/")
        );
        assert_eq!(
            expanded.fields.get("sso_region").map(String::as_str),
            Some("us-east-1")
        );
        // The reference field itself is preserved.
        assert_eq!(
            expanded.fields.get("session_ref").map(String::as_str),
            Some(referenced_id.to_string().as_str())
        );
    }

    #[test]
    fn consumer_overrides_referenced_values() {
        let referenced = profile_with(
            "ref-provider",
            &[("sso_start_url", "https://default.awsapps.com/start/")],
        );
        let referenced_id = referenced.id;

        let consumer = profile_with(
            "consumer-provider",
            &[
                ("session_ref", &referenced_id.to_string()),
                ("sso_start_url", "https://override.awsapps.com/start/"),
            ],
        );

        let form_def = form_with_ref_field("session_ref", "ref-provider");

        let expanded =
            expand_auth_profile_refs(&consumer, &form_def, &|_| Some(referenced.clone()));

        assert_eq!(
            expanded.fields.get("sso_start_url").map(String::as_str),
            Some("https://override.awsapps.com/start/"),
            "consumer's non-empty value must win over the referenced profile"
        );
    }

    #[test]
    fn empty_ref_value_is_noop() {
        let consumer = profile_with("consumer-provider", &[("session_ref", "")]);

        let form_def = form_with_ref_field("session_ref", "ref-provider");

        let expanded = expand_auth_profile_refs(&consumer, &form_def, &|_| {
            panic!("lookup should not be called for empty ref")
        });

        assert!(!expanded.fields.contains_key("sso_start_url"));
    }

    #[test]
    fn unresolved_ref_is_silently_skipped() {
        let consumer = profile_with(
            "consumer-provider",
            &[("session_ref", "00000000-0000-0000-0000-000000000001")],
        );

        let form_def = form_with_ref_field("session_ref", "ref-provider");

        let expanded = expand_auth_profile_refs(&consumer, &form_def, &|_| None);

        assert!(!expanded.fields.contains_key("sso_start_url"));
        assert_eq!(
            expanded.fields.get("session_ref").map(String::as_str),
            Some("00000000-0000-0000-0000-000000000001")
        );
    }

    #[test]
    fn non_ref_fields_are_ignored() {
        let consumer = profile_with("consumer-provider", &[("some_text_field", "value")]);

        let form_def = AuthFormDef {
            tabs: vec![FormTab {
                id: "main".to_string(),
                label: "Main".to_string(),
                sections: vec![FormSection {
                    title: "Section".to_string(),
                    fields: vec![FormFieldDef {
                        id: "some_text_field".to_string(),
                        label: "Text".to_string(),
                        kind: FormFieldKind::Text,
                        placeholder: String::new(),
                        required: false,
                        default_value: String::new(),
                        enabled_when_checked: None,
                        enabled_when_unchecked: None,
                        disabled_when_field_set: None,
                        help: None,
                    }],
                }],
            }],
        };

        let expanded = expand_auth_profile_refs(&consumer, &form_def, &|_| {
            panic!("lookup should not be called when no AuthProfileRef fields")
        });

        assert_eq!(
            expanded.fields.get("some_text_field").map(String::as_str),
            Some("value")
        );
    }
}
