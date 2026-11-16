use std::time::Duration;

use dbflux_transfer::sql_script::OnError;

use super::super::options::{
    ConnectionRules, ScriptOptions, TransactionAvailability, elapsed_label, group_digits,
    read_only_conflict, size_label,
};
use super::summary_with;

fn rules(transactions_supported: bool, read_only_profile: bool) -> ConnectionRules {
    ConnectionRules {
        transactions_supported,
        read_only_profile,
        self_managed: None,
    }
}

#[test]
fn the_defaults_are_stop_at_the_first_error_in_one_transaction() {
    let options = ScriptOptions::default();

    assert_eq!(options.on_error, OnError::Stop);
    assert!(options.transaction);
    assert!(options.runs_in_transaction(&rules(true, false)));
}

#[test]
fn the_transaction_is_available_only_with_transactions_and_stop() {
    let stop = ScriptOptions::default();
    let continuing = ScriptOptions {
        on_error: OnError::Continue,
        ..ScriptOptions::default()
    };

    assert_eq!(
        stop.availability(&rules(true, false)),
        TransactionAvailability::Available
    );
    assert_eq!(
        continuing.availability(&rules(true, false)),
        TransactionAvailability::OffForContinue
    );
    assert_eq!(
        stop.availability(&rules(false, false)),
        TransactionAvailability::Unavailable
    );
    assert_eq!(
        continuing.availability(&rules(false, false)),
        TransactionAvailability::Unavailable,
        "a connection without transactions says so before it blames Continue"
    );
}

#[test]
fn the_run_options_follow_the_availability_not_the_checkbox() {
    let continuing = ScriptOptions {
        on_error: OnError::Continue,
        transaction: true,
    };
    let run = continuing.run_options(&rules(true, true));

    assert_eq!(run.on_error, OnError::Continue);
    assert!(!run.transaction, "Continue turns the transaction off");
    assert!(run.read_only_profile);

    let unchecked = ScriptOptions {
        on_error: OnError::Stop,
        transaction: false,
    };
    assert!(!unchecked.run_options(&rules(true, false)).transaction);
    assert!(
        !ScriptOptions::default()
            .run_options(&rules(false, false))
            .transaction
    );
    assert!(
        ScriptOptions::default()
            .run_options(&rules(true, false))
            .transaction
    );
}

#[test]
fn a_read_only_profile_refuses_a_script_that_changes_data() {
    let changing = summary_with(3, 2);
    let reading = summary_with(3, 0);

    assert_eq!(read_only_conflict(&changing, &rules(true, true)), Some(2));
    assert_eq!(read_only_conflict(&changing, &rules(true, false)), None);
    assert_eq!(read_only_conflict(&reading, &rules(true, true)), None);
}

#[test]
fn sizes_read_in_bytes_kilobytes_and_megabytes() {
    assert_eq!(size_label(812), "812 B");
    assert_eq!(size_label(2048), "2.0 KB");
    assert_eq!(size_label(4_404_019), "4.2 MB");
    assert_eq!(size_label(240 * 1024 * 1024), "240 MB");
    assert_eq!(size_label(100 * 1024 * 1024), "100 MB");
}

#[test]
fn digits_group_in_threes() {
    assert_eq!(group_digits(0, ","), "0");
    assert_eq!(group_digits(999, ","), "999");
    assert_eq!(group_digits(12_481, ","), "12,481");
    assert_eq!(group_digits(1_234_567, "."), "1.234.567");
}

#[test]
fn elapsed_time_has_one_decimal() {
    assert_eq!(elapsed_label(Duration::from_millis(8_400)), "8.4 s");
    assert_eq!(elapsed_label(Duration::from_millis(12)), "0.0 s");
}

#[test]
fn a_script_that_manages_its_own_transactions_turns_the_option_off() {
    let own = ConnectionRules {
        self_managed: Some(("COMMIT", 214)),
        ..rules(true, false)
    };
    let options = ScriptOptions::default();

    assert_eq!(
        options.availability(&own),
        TransactionAvailability::SelfManaged
    );
    assert!(!options.run_options(&own).transaction, "it runs as written");

    let without_transactions = ConnectionRules {
        self_managed: Some(("COMMIT", 214)),
        ..rules(false, false)
    };
    assert_eq!(
        options.availability(&without_transactions),
        TransactionAvailability::SelfManaged,
        "the script's own COMMIT decides what is saved, with or without transactions"
    );
}
