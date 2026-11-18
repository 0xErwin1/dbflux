//! The catalog keys of the import wizard's "Run SQL script" flow. They live
//! beside the inline `tests` module because that module is already large.

/// Every key of the flow, in the order the catalogs declare them.
const SCRIPT_KEYS: &[&str] = &[
    "document.import_wizard.script.error.session_open_failed",
    "document.import_wizard.script.error.session_cleanup_failed",
    "document.import_wizard.script.done.stopped_own_transaction_next",
    "document.import_wizard.script.done.cancelled_own_transaction_next",
    "document.import_wizard.script.error.interrupt_failed",
    "document.import_wizard.script.title",
    "document.import_wizard.script.task",
    "document.import_wizard.script.thousands_separator",
    "document.import_wizard.script.classification.metadata",
    "document.import_wizard.script.classification.read",
    "document.import_wizard.script.classification.write",
    "document.import_wizard.script.classification.destructive",
    "document.import_wizard.script.classification.admin",
    "document.import_wizard.script.classification.admin_safe",
    "document.import_wizard.script.classification.admin_destructive",
    "document.import_wizard.script.error.unreadable",
    "document.import_wizard.script.error.unreadable_next",
    "document.import_wizard.script.error.not_utf8",
    "document.import_wizard.script.error.not_utf8_next",
    "document.import_wizard.script.error.too_large",
    "document.import_wizard.script.error.too_large_next",
    "document.import_wizard.script.error.empty",
    "document.import_wizard.script.error.empty_next",
    "document.import_wizard.script.error.not_sql",
    "document.import_wizard.script.error.not_sql_next",
    "document.import_wizard.script.error.other_next",
    "document.import_wizard.script.error.read_only.one",
    "document.import_wizard.script.error.read_only.many",
    "document.import_wizard.script.error.read_only_next",
    "document.import_wizard.script.configure.file",
    "document.import_wizard.script.configure.script_label",
    "document.import_wizard.script.configure.script_summary.one",
    "document.import_wizard.script.configure.script_summary.many",
    "document.import_wizard.script.configure.preview_label",
    "document.import_wizard.script.configure.preview_more.one",
    "document.import_wizard.script.configure.preview_more.many",
    "document.import_wizard.script.configure.preview_hint",
    "document.import_wizard.script.configure.no_where",
    "document.import_wizard.script.configure.on_error_label",
    "document.import_wizard.script.configure.on_error_stop",
    "document.import_wizard.script.configure.on_error_continue",
    "document.import_wizard.script.configure.on_error_stop_hint",
    "document.import_wizard.script.configure.on_error_continue_hint",
    "document.import_wizard.script.configure.transaction_label",
    "document.import_wizard.script.configure.transaction_checkbox",
    "document.import_wizard.script.configure.transaction_hint",
    "document.import_wizard.script.configure.transaction_unchecked_hint",
    "document.import_wizard.script.configure.transaction_off_continue",
    "document.import_wizard.script.configure.transaction_unavailable",
    "document.import_wizard.script.configure.ddl_commits",
    "document.import_wizard.script.footer.read_only",
    "document.import_wizard.script.footer.running_transaction",
    "document.import_wizard.script.footer.running_kept",
    "document.import_wizard.script.footer.cancelling_transaction",
    "document.import_wizard.script.footer.cancelling_kept",
    "document.import_wizard.script.confirm.destructive_lead.one",
    "document.import_wizard.script.confirm.destructive_lead.many",
    "document.import_wizard.script.confirm.plain_lead.one",
    "document.import_wizard.script.confirm.plain_lead.many",
    "document.import_wizard.script.confirm.warning",
    "document.import_wizard.script.confirm.danger_line",
    "document.import_wizard.script.confirm.first_label",
    "document.import_wizard.script.confirm.first_line",
    "document.import_wizard.script.confirm.how_transaction",
    "document.import_wizard.script.confirm.how_each_stop",
    "document.import_wizard.script.confirm.how_each_continue",
    "document.import_wizard.script.confirm.run",
    "document.import_wizard.script.confirm.proceed",
    "document.import_wizard.script.running.title",
    "document.import_wizard.script.running.current",
    "document.import_wizard.script.running.starting",
    "document.import_wizard.script.running.transaction_line",
    "document.import_wizard.script.running.no_transaction_line",
    "document.import_wizard.script.running.cancel",
    "document.import_wizard.script.running.cancelling",
    "document.import_wizard.script.toast.completed",
    "document.import_wizard.script.done.finished.one",
    "document.import_wizard.script.done.finished.many",
    "document.import_wizard.script.done.committed",
    "document.import_wizard.script.done.saved_as_run",
    "document.import_wizard.script.done.with_errors",
    "document.import_wizard.script.done.with_errors_next",
    "document.import_wizard.script.done.failure_label",
    "document.import_wizard.script.done.cancelled",
    "document.import_wizard.script.done.rolled_back_cancelled",
    "document.import_wizard.script.done.cancelled_rolled_back_next",
    "document.import_wizard.script.done.nothing_ran",
    "document.import_wizard.script.done.cancelled_kept.one",
    "document.import_wizard.script.done.cancelled_kept.many",
    "document.import_wizard.script.done.cancelled_kept_next",
    "document.import_wizard.script.done.stopped",
    "document.import_wizard.script.done.failed_statement",
    "document.import_wizard.script.done.database_error",
    "document.import_wizard.script.done.rolled_back_before.one",
    "document.import_wizard.script.done.rolled_back_before.many",
    "document.import_wizard.script.done.rolled_back_first",
    "document.import_wizard.script.done.rolled_back_nothing_saved",
    "document.import_wizard.script.done.stopped_rolled_back_next",
    "document.import_wizard.script.done.kept_before.one",
    "document.import_wizard.script.done.kept_before.many",
    "document.import_wizard.script.done.nothing_before_it",
    "document.import_wizard.script.done.not_run_range",
    "document.import_wizard.script.done.not_run_single",
    "document.import_wizard.script.done.stopped_kept_next",
    "document.import_wizard.script.done.use_transaction_next",
    "document.import_wizard.script.done.fix_first_next",
    "document.import_wizard.script.done.commit_failed",
    "document.import_wizard.script.done.commit_failed_next",
    "document.import_wizard.script.done.unresolved",
    "document.import_wizard.script.done.unresolved_message",
    "document.import_wizard.script.done.unresolved_next",
    "document.import_wizard.script.done.ddl_not_undone",
    "document.import_wizard.script.done.not_run",
    "document.import_wizard.script.done.not_run_next",
    "document.import_wizard.script.error.client_directive",
    "document.import_wizard.script.error.client_directive_next",
    "document.import_wizard.script.error.split_routine",
    "document.import_wizard.script.error.split_routine_next",
    "document.import_wizard.script.error.copy_stdin",
    "document.import_wizard.script.error.copy_stdin_next",
    "document.import_wizard.script.error.psql_meta",
    "document.import_wizard.script.error.psql_meta_next",
    "document.import_wizard.script.error.self_managed",
    "document.import_wizard.script.error.self_managed_next",
    "document.import_wizard.script.configure.transaction_self_managed_commit",
    "document.import_wizard.script.configure.transaction_self_managed",
    "document.import_wizard.script.configure.preview_sequence_hint",
    "document.import_wizard.script.confirm.how_self_stop",
    "document.import_wizard.script.confirm.how_self_continue",
    "document.import_wizard.script.error.session_commit",
    "document.import_wizard.script.error.session_commit_next",
    "document.import_wizard.script.error.session_change_autocommit",
    "document.import_wizard.script.error.session_change_implicit",
    "document.import_wizard.script.done.partially_kept",
    "document.import_wizard.script.done.partially_kept_no_line",
    "document.import_wizard.script.done.partially_kept_next",
    "document.import_wizard.script.done.script_rolled_back",
    "document.import_wizard.script.done.ended_without_commit",
    "document.import_wizard.script.done.ended_without_commit_next",
    "document.import_wizard.script.done.ended_without_commit_next_or",
    "document.import_wizard.script.done.left_open",
    "document.import_wizard.script.done.left_open_message",
    "document.import_wizard.script.done.left_open_next",
    "document.import_wizard.script.error.conditional_transaction",
    "document.import_wizard.script.error.conditional_transaction_next",
    "document.import_wizard.script.done.possibly_open_rolled_back",
    "document.import_wizard.pick_file.description_sql",
    "document.import_wizard.pick_file.filter_label_sql",
    "document.import_wizard.pick_file.error.unsupported_sql",
];

/// The `%{name}` placeholders of a catalog value, sorted, repeats kept.
fn delimited_placeholders(text: &str) -> Vec<&str> {
    let mut placeholders: Vec<&str> = text
        .match_indices("%{")
        .filter_map(|(start, _)| {
            let end = text[start..].find('}')?;
            Some(&text[start..=start + end])
        })
        .collect();

    placeholders.sort_unstable();
    placeholders
}

/// Every key resolves in all five locales, and each translation keeps exactly
/// the placeholders of the English text, none renamed, lost or added. A
/// translation may equal the English text: `%{count} × %{message}` and the
/// thousands separator are the same in several languages.
#[test]
fn import_wizard_script_keys_resolve_in_every_locale() {
    for key in SCRIPT_KEYS {
        let english = dbflux_i18n::translate_in("en", key);
        assert_ne!(english, *key, "{key} missing from en");

        for locale in ["es", "ko", "pt_BR", "zh_Hans"] {
            let value = dbflux_i18n::translate_in(locale, key);

            assert!(!value.is_empty(), "{key} resolved empty in {locale}");
            assert_ne!(value, *key, "{key} missing from {locale}");
            assert_ne!(
                value,
                format!("{locale}.{key}"),
                "{key} missing from {locale}"
            );
            assert_eq!(
                delimited_placeholders(&value),
                delimited_placeholders(&english),
                "{key} in {locale} must keep the placeholders of the English text"
            );
        }
    }
}
