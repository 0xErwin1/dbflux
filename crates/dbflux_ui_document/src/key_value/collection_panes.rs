//! Ranged value panes for sorted sets and streams.
//!
//! Instead of reading the whole collection with the key, these panes page
//! through it: sorted sets by rank (highest or lowest first), streams by
//! entry ID between a start and an end bound (newest or oldest first), with
//! the stream's consumer groups alongside.

use super::parsing::MemberEntry;
use chrono::{DateTime, Local, TimeZone};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_core::{
    DbError, KeyEntry, KeyGetResult, KeyLoadState, KeyMetadata, KeyMetadataRequest, KeyType,
    KeyValueFeatures, RangeOrder, StreamClaimRequest, StreamConsumerGroup, StreamEntry,
    StreamGroupsRequest, StreamPendingEntry, StreamPendingRequest, StreamRangeRequest, TaskKind,
    ValueRepr, ZSetMember, ZSetRangeRequest,
};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error_async};
use gpui::*;

/// Sorted-set members fetched per page.
pub(super) const ZSET_PAGE_SIZE: u64 = 100;

/// Stream entries fetched per page when the connection sets no preview limit.
const DEFAULT_STREAM_PAGE_SIZE: u64 = 50;

/// Pending entries listed or claimed at once.
pub(super) const PENDING_PAGE_SIZE: u64 = 100;

/// Field columns the stream table shows before it stops adding more.
pub(super) const MAX_STREAM_FIELD_COLUMNS: usize = 6;

/// Sorted-set pane: a page of members by rank.
pub(super) struct ZSetPane {
    pub order: RangeOrder,
    pub members: Vec<ZSetMember>,
    pub total: u64,
    pub loading: bool,
}

impl ZSetPane {
    fn new() -> Self {
        Self {
            order: RangeOrder::Descending,
            members: Vec::new(),
            total: 0,
            loading: false,
        }
    }

    /// The top score of the set in the current order, used as the 100 %
    /// mark of the relative bars.
    pub(super) fn reference_scores(&self) -> (f64, f64) {
        let top = self
            .members
            .iter()
            .map(|member| member.score)
            .fold(f64::NEG_INFINITY, f64::max);
        let bottom = self
            .members
            .iter()
            .map(|member| member.score)
            .fold(f64::INFINITY, f64::min);

        (top, bottom)
    }
}

/// Stream pane: a page of entries plus the stream's consumer groups.
pub(super) struct StreamPane {
    pub order: RangeOrder,
    pub start_input: Entity<InputState>,
    pub end_input: Entity<InputState>,
    pub entries: Vec<StreamEntry>,
    pub total: u64,
    pub loading: bool,
    pub groups: Vec<StreamConsumerGroup>,
    pub groups_loaded: bool,
    /// Pending entries of one group, when the user asked to see them.
    pub pending: Option<(String, Vec<StreamPendingEntry>)>,
    /// Target consumer field of the claim form, per group.
    pub claim: Option<ClaimForm>,
    _subscriptions: Vec<Subscription>,
}

pub(super) struct ClaimForm {
    pub group: String,
    pub consumer_input: Entity<InputState>,
    _subscription: Subscription,
}

/// Fraction of the bar filled for `score`: relative to the top score when
/// scores are positive, otherwise spread between the lowest and highest.
pub(super) fn relative_bar_fraction(score: f64, top: f64, bottom: f64) -> f32 {
    if !score.is_finite() || !top.is_finite() || !bottom.is_finite() {
        return 0.0;
    }

    let fraction = if bottom >= 0.0 && top > 0.0 {
        score / top
    } else if top > bottom {
        (score - bottom) / (top - bottom)
    } else {
        1.0
    };

    fraction.clamp(0.0, 1.0) as f32
}

/// Score as the table shows it: the integer part grouped by thousands
/// (`4,954`, `48,210.5`) and up to four decimals with trailing zeros trimmed.
pub(super) fn format_score(score: f64) -> String {
    let text = format!("{:.4}", score.abs());
    let text = text.trim_end_matches('0').trim_end_matches('.');

    let (integer, fraction) = match text.split_once('.') {
        Some((integer, fraction)) => (integer, Some(fraction)),
        None => (text, None),
    };

    let grouped = integer
        .parse::<u64>()
        .map(super::key_tree::group_thousands)
        .unwrap_or_else(|_| integer.to_string());

    let sign = if score < 0.0 { "-" } else { "" };

    match fraction {
        Some(fraction) => format!("{sign}{grouped}.{fraction}"),
        None => format!("{sign}{grouped}"),
    }
}

/// Range bounds for the next stream page: the page continues past the last
/// loaded entry with an exclusive bound on the side it is reading towards.
pub(super) fn next_stream_bounds(
    order: RangeOrder,
    start: &str,
    end: &str,
    last_loaded_id: Option<&str>,
) -> (String, String) {
    match (order, last_loaded_id) {
        (_, None) => (start.to_string(), end.to_string()),
        (RangeOrder::Descending, Some(last)) => (start.to_string(), format!("({last}")),
        (RangeOrder::Ascending, Some(last)) => (format!("({last}"), end.to_string()),
    }
}

/// Start bound typed in the range field, or `-` when blank.
pub(super) fn stream_start_bound(text: &str) -> String {
    bound_or(text, "-")
}

/// End bound typed in the range field, or `+` when blank.
pub(super) fn stream_end_bound(text: &str) -> String {
    bound_or(text, "+")
}

fn bound_or(text: &str, fallback: &str) -> String {
    let trimmed = text.trim();

    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

/// Field names across the loaded entries, in first-seen order, capped at
/// [`MAX_STREAM_FIELD_COLUMNS`].
pub(super) fn stream_field_columns(entries: &[StreamEntry]) -> Vec<String> {
    let mut columns: Vec<String> = Vec::new();

    for entry in entries {
        for (field, _) in &entry.fields {
            if columns.len() >= MAX_STREAM_FIELD_COLUMNS {
                return columns;
            }

            if !columns.iter().any(|column| column == field) {
                columns.push(field.clone());
            }
        }
    }

    columns
}

/// Time of a stream entry from the millisecond part of its ID: `today
/// 17:23:20` for today's entries, a full local date otherwise.
pub(super) fn entry_time_label<Tz: TimeZone>(entry_id: &str, now: &DateTime<Tz>) -> Option<String>
where
    Tz::Offset: std::fmt::Display,
{
    let milliseconds: i64 = entry_id.split('-').next()?.parse().ok()?;
    let at = now.timezone().timestamp_millis_opt(milliseconds).single()?;

    if at.date_naive() == now.date_naive() {
        Some(dbflux_i18n::t!(
            "document.key_value.stream.time_today",
            time = at.format("%H:%M:%S").to_string()
        ))
    } else {
        Some(at.format("%Y-%m-%d %H:%M:%S").to_string())
    }
}

/// Shortened entry ID for narrow columns (`…94866-0`).
pub(super) fn short_entry_id(entry_id: &str) -> String {
    const KEEP: usize = 7;

    let characters: Vec<char> = entry_id.chars().collect();
    if characters.len() <= KEEP + 1 {
        return entry_id.to_string();
    }

    let tail: String = characters
        .get(characters.len() - KEEP..)
        .unwrap_or_default()
        .iter()
        .collect();
    format!("…{tail}")
}

/// Idle time in minutes or hours for the pending callout.
pub(super) fn idle_label(idle_ms: u64) -> String {
    super::metadata::format_ttl(Some(idle_ms / 1000))
}

/// The group to call out: the one with the most pending entries.
pub(super) fn busiest_group(groups: &[StreamConsumerGroup]) -> Option<&StreamConsumerGroup> {
    groups
        .iter()
        .filter(|group| group.pending > 0)
        .max_by_key(|group| group.pending)
}

fn zset_members_as_rows(members: &[ZSetMember]) -> Vec<MemberEntry> {
    members
        .iter()
        .map(|member| MemberEntry {
            display: member.member.clone(),
            field: None,
            score: Some(member.score),
            entry_id: None,
        })
        .collect()
}

fn stream_entries_as_rows(entries: &[StreamEntry]) -> Vec<MemberEntry> {
    entries
        .iter()
        .map(|entry| {
            let fields: serde_json::Map<String, serde_json::Value> = entry
                .fields
                .iter()
                .map(|(field, value)| (field.clone(), serde_json::Value::String(value.clone())))
                .collect();

            MemberEntry {
                display: entry.id.clone(),
                field: serde_json::to_string(&fields).ok(),
                score: None,
                entry_id: Some(entry.id.clone()),
            }
        })
        .collect()
}

/// Value stand-in for a key opened through a ranged pane: the header reads
/// the entry, and the pane holds the data.
fn ranged_value(key: &str, key_type: KeyType, metadata: Option<&KeyMetadata>) -> KeyGetResult {
    KeyGetResult {
        entry: KeyEntry {
            key: key.to_string(),
            key_type: Some(key_type),
            ttl_seconds: metadata.and_then(|metadata| metadata.ttl_seconds),
            size_bytes: metadata.and_then(|metadata| metadata.size_bytes),
        },
        value: Vec::new(),
        repr: if key_type == KeyType::Stream {
            ValueRepr::Stream
        } else {
            ValueRepr::Structured
        },
        load_state: KeyLoadState::Loaded,
    }
}

impl super::KeyValueDocument {
    /// Whether `key_type` opens in a ranged pane on this connection.
    pub(super) fn uses_ranged_pane(&self, key_type: Option<KeyType>) -> bool {
        match key_type {
            Some(KeyType::SortedSet) => self
                .key_features
                .contains(KeyValueFeatures::SORTED_SET_RANGE),
            Some(KeyType::Stream) => self.key_features.contains(KeyValueFeatures::STREAM_RANGE),
            _ => false,
        }
    }

    fn stream_page_size(&self, cx: &App) -> u64 {
        self.app_state
            .read(cx)
            .effective_settings_for_connection(Some(self.profile_id))
            .driver_values
            .get("stream_preview_limit")
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_STREAM_PAGE_SIZE)
    }

    /// Opens a sorted-set or stream key through its ranged pane: metadata
    /// for the header plus the first page, in one background task.
    pub(super) fn load_ranged_key(
        &mut self,
        key: String,
        key_type: KeyType,
        window: Option<&mut Window>,
        cx: &mut Context<Self>,
    ) {
        match key_type {
            KeyType::SortedSet => {
                self.stream_pane = None;
                if self.zset_pane.is_none() {
                    self.zset_pane = Some(ZSetPane::new());
                }
            }
            KeyType::Stream => {
                self.zset_pane = None;
                if self.stream_pane.is_none() {
                    let Some(window) = window else {
                        self.pending_stream_pane = true;
                        cx.notify();
                        return;
                    };
                    self.stream_pane = Some(self.new_stream_pane(window, cx));
                }
            }
            _ => return,
        }

        self.load_ranged_page(key, key_type, false, cx);
    }

    fn new_stream_pane(&mut self, window: &mut Window, cx: &mut Context<Self>) -> StreamPane {
        let start_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.key_value.stream.start_placeholder"
            ))
        });
        let end_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("document.key_value.stream.end_placeholder"))
        });

        let subscriptions = [&start_input, &end_input]
            .into_iter()
            .map(|input| {
                cx.subscribe_in(input, window, |this, _, event: &InputEvent, _, cx| {
                    if let InputEvent::PressEnter { .. } = event {
                        this.reload_stream_range(cx);
                    }
                })
            })
            .collect();

        StreamPane {
            order: RangeOrder::Descending,
            start_input,
            end_input,
            entries: Vec::new(),
            total: 0,
            loading: false,
            groups: Vec::new(),
            groups_loaded: false,
            pending: None,
            claim: None,
            _subscriptions: subscriptions,
        }
    }

    /// Materialises the stream pane once a window is available, for a
    /// stream key selected from a code path without one.
    pub(super) fn flush_pending_stream_pane(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !std::mem::take(&mut self.pending_stream_pane) {
            return;
        }

        let Some(key) = self.selected_key() else {
            return;
        };

        self.stream_pane = Some(self.new_stream_pane(window, cx));
        self.load_ranged_page(key, KeyType::Stream, false, cx);
    }

    pub(super) fn set_zset_order(&mut self, order: RangeOrder, cx: &mut Context<Self>) {
        let Some(pane) = self.zset_pane.as_mut() else {
            return;
        };

        if pane.order == order {
            return;
        }

        pane.order = order;
        self.reload_ranged_first_page(cx);
    }

    pub(super) fn set_stream_order(&mut self, order: RangeOrder, cx: &mut Context<Self>) {
        let Some(pane) = self.stream_pane.as_mut() else {
            return;
        };

        if pane.order == order {
            return;
        }

        pane.order = order;
        self.reload_ranged_first_page(cx);
    }

    pub(super) fn reload_stream_range(&mut self, cx: &mut Context<Self>) {
        self.reload_ranged_first_page(cx);
    }

    fn reload_ranged_first_page(&mut self, cx: &mut Context<Self>) {
        let Some(key) = self.selected_key() else {
            return;
        };
        let Some(key_type) = self.selected_key_type() else {
            return;
        };

        self.load_ranged_page(key, key_type, false, cx);
    }

    /// Appends the next page of the open sorted set or stream.
    pub(super) fn load_more_ranged(&mut self, cx: &mut Context<Self>) {
        let Some(key) = self.selected_key() else {
            return;
        };
        let Some(key_type) = self.selected_key_type() else {
            return;
        };

        self.load_ranged_page(key, key_type, true, cx);
    }

    pub(super) fn can_load_more_ranged(&self) -> bool {
        match (&self.zset_pane, &self.stream_pane) {
            (Some(pane), _) => !pane.loading && (pane.members.len() as u64) < pane.total,
            (_, Some(pane)) => !pane.loading && (pane.entries.len() as u64) < pane.total,
            _ => false,
        }
    }

    fn load_ranged_page(
        &mut self,
        key: String,
        key_type: KeyType,
        append: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(connection) = self.get_connection(cx) else {
            self.report_connection_inactive(cx);
            return;
        };

        let keyspace = self.keyspace_index();
        let include_metadata =
            !append && self.key_features.contains(KeyValueFeatures::KEY_METADATA);
        let stream_page_size = self.stream_page_size(cx);

        let range = match key_type {
            KeyType::SortedSet => {
                let Some(pane) = self.zset_pane.as_mut() else {
                    return;
                };
                pane.loading = true;

                RangedRequest::ZSet(ZSetRangeRequest {
                    key: key.clone(),
                    keyspace,
                    offset: if append { pane.members.len() as u64 } else { 0 },
                    count: ZSET_PAGE_SIZE,
                    order: pane.order,
                })
            }
            KeyType::Stream => {
                let Some(pane) = self.stream_pane.as_mut() else {
                    return;
                };
                pane.loading = true;

                let start = stream_start_bound(&pane.start_input.read(cx).value());
                let end = stream_end_bound(&pane.end_input.read(cx).value());
                let last_loaded = if append {
                    pane.entries.last().map(|entry| entry.id.as_str())
                } else {
                    None
                };
                let (start, end) = next_stream_bounds(pane.order, &start, &end, last_loaded);

                RangedRequest::Stream(StreamRangeRequest {
                    key: key.clone(),
                    keyspace,
                    start,
                    end,
                    count: stream_page_size,
                    order: pane.order,
                })
            }
            _ => return,
        };

        let description = match &range {
            RangedRequest::ZSet(request) => format!(
                "ZRANGE {} {} {}",
                dbflux_core::truncate_string_safe(&key, 60),
                request.offset,
                request.offset + request.count.saturating_sub(1)
            ),
            RangedRequest::Stream(request) => format!(
                "XRANGE {} {} {} COUNT {}",
                dbflux_core::truncate_string_safe(&key, 60),
                request.start,
                request.end,
                request.count
            ),
        };

        let (task_id, cancel_token) = self.runner.start_primary(TaskKind::KeyGet, description, cx);
        let entity = cx.entity().clone();
        cx.notify();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn({
                    let key = key.clone();
                    async move {
                        let api = connection.key_value_api().ok_or_else(|| {
                            DbError::NotSupported("Key-value API unavailable".to_string())
                        })?;

                        let metadata = if include_metadata {
                            api.key_metadata(&KeyMetadataRequest {
                                keys: vec![key],
                                keyspace,
                                include_encoding: true,
                            })
                            .ok()
                            .and_then(|mut items| items.pop())
                        } else {
                            None
                        };

                        let page = match range {
                            RangedRequest::ZSet(request) => {
                                RangedPage::ZSet(api.zset_range(&request)?)
                            }
                            RangedRequest::Stream(request) => {
                                RangedPage::Stream(api.stream_range(&request)?)
                            }
                        };

                        Ok::<_, DbError>((metadata, page))
                    }
                })
                .await;

            if let Err(error) = &result {
                report_error_async(
                    UserFacingError::new(ErrorKind::Driver, error.to_string()),
                    cx,
                );
            }

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if cancel_token.is_cancelled() || this.selected_key().as_deref() != Some(&key) {
                        return;
                    }

                    match result {
                        Ok((metadata, page)) => {
                            this.runner.complete_primary(task_id, cx);
                            this.apply_ranged_page(&key, key_type, append, metadata, page, cx);
                        }
                        Err(error) => {
                            this.runner.fail_primary(task_id, error.to_string(), cx);

                            if let Some(pane) = this.zset_pane.as_mut() {
                                pane.loading = false;
                            }
                            if let Some(pane) = this.stream_pane.as_mut() {
                                pane.loading = false;
                            }
                        }
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }

    fn apply_ranged_page(
        &mut self,
        key: &str,
        key_type: KeyType,
        append: bool,
        metadata: Option<KeyMetadata>,
        page: RangedPage,
        cx: &mut Context<Self>,
    ) {
        if !append {
            let value = ranged_value(key, key_type, metadata.as_ref());
            self.apply_ttl_from_entry(&value.entry, cx);
            self.selected_value = Some(value);
            self.value_metadata = metadata;
            self.value_view_mode = super::KvValueViewMode::Table;
        }

        match page {
            RangedPage::ZSet(page) => {
                if let Some(pane) = self.zset_pane.as_mut() {
                    if append {
                        pane.members.extend(page.members);
                    } else {
                        pane.members = page.members;
                    }
                    pane.total = page.total;
                    pane.loading = false;
                }
            }
            RangedPage::Stream(page) => {
                if let Some(pane) = self.stream_pane.as_mut() {
                    if append {
                        pane.entries.extend(page.entries);
                    } else {
                        pane.entries = page.entries;
                    }
                    pane.total = page.total;
                    pane.loading = false;
                }

                if !append && self.key_features.contains(KeyValueFeatures::STREAM_GROUPS) {
                    self.load_stream_groups(cx);
                }
            }
        }

        self.rebuild_cached_members(cx);
    }

    /// Member rows for the ranged panes, in the shape the member editors and
    /// the document view expect.
    pub(super) fn ranged_member_rows(&self) -> Option<Vec<MemberEntry>> {
        if let Some(pane) = &self.zset_pane {
            return Some(zset_members_as_rows(&pane.members));
        }

        if let Some(pane) = &self.stream_pane {
            return Some(stream_entries_as_rows(&pane.entries));
        }

        None
    }

    pub(super) fn load_stream_groups(&mut self, cx: &mut Context<Self>) {
        let Some(key) = self.selected_key() else {
            return;
        };
        let Some(connection) = self.get_connection(cx) else {
            return;
        };

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn({
                    let key = key.clone();
                    async move {
                        let api = connection.key_value_api().ok_or_else(|| {
                            DbError::NotSupported("Key-value API unavailable".to_string())
                        })?;
                        api.stream_groups(&StreamGroupsRequest { key, keyspace })
                    }
                })
                .await;

            if let Err(error) = &result {
                report_error_async(
                    UserFacingError::new(ErrorKind::Driver, error.to_string()),
                    cx,
                );
            }

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if this.selected_key().as_deref() != Some(&key) {
                        return;
                    }

                    if let Some(pane) = this.stream_pane.as_mut() {
                        pane.groups = result.unwrap_or_default();
                        pane.groups_loaded = true;
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }

    /// Lists the oldest pending entries of `group` under the groups table.
    pub(super) fn view_pending_entries(&mut self, group: String, cx: &mut Context<Self>) {
        let Some(key) = self.selected_key() else {
            return;
        };
        let Some(connection) = self.get_connection(cx) else {
            self.report_connection_inactive(cx);
            return;
        };

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn({
                    let key = key.clone();
                    let group = group.clone();
                    async move {
                        let api = connection.key_value_api().ok_or_else(|| {
                            DbError::NotSupported("Key-value API unavailable".to_string())
                        })?;
                        api.stream_pending(&StreamPendingRequest {
                            key,
                            group,
                            keyspace,
                            count: PENDING_PAGE_SIZE,
                        })
                    }
                })
                .await;

            match result {
                Ok(entries) => {
                    cx.update(|cx| {
                        entity.update(cx, |this, cx| {
                            if this.selected_key().as_deref() != Some(&key) {
                                return;
                            }

                            if let Some(pane) = this.stream_pane.as_mut() {
                                pane.pending = Some((group, entries));
                            }
                            cx.notify();
                        });
                    });
                }
                Err(error) => report_error_async(
                    UserFacingError::new(ErrorKind::Driver, error.to_string()),
                    cx,
                ),
            }
        })
        .detach();
    }

    /// Opens the claim form for `group`, prefilled with one of its readers.
    pub(super) fn open_claim_form(
        &mut self,
        group: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(pane) = self.stream_pane.as_ref() else {
            return;
        };

        let suggested = pane
            .groups
            .iter()
            .find(|candidate| candidate.name == group)
            .and_then(|candidate| candidate.consumers.first().cloned())
            .unwrap_or_default();

        let consumer_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.key_value.stream.claim_placeholder"
            ));
            state.set_value(suggested, window, cx);
            state
        });
        consumer_input.update(cx, |state, cx| state.focus(window, cx));

        let subscription = cx.subscribe_in(
            &consumer_input,
            window,
            |this, _, event: &InputEvent, _, cx| {
                if let InputEvent::PressEnter { .. } = event {
                    this.claim_pending_entries(cx);
                }
            },
        );

        if let Some(pane) = self.stream_pane.as_mut() {
            pane.claim = Some(ClaimForm {
                group,
                consumer_input,
                _subscription: subscription,
            });
        }

        self.focus_mode = super::KeyValueFocusMode::TextInput;
        cx.notify();
    }

    pub(super) fn close_claim_form(&mut self, cx: &mut Context<Self>) {
        if let Some(pane) = self.stream_pane.as_mut() {
            pane.claim = None;
        }
        cx.notify();
    }

    /// Moves the group's pending entries to the consumer typed in the claim
    /// form (`XCLAIM ... JUSTID`), then refreshes the groups.
    pub(super) fn claim_pending_entries(&mut self, cx: &mut Context<Self>) {
        let Some(key) = self.selected_key() else {
            return;
        };
        let Some(pane) = self.stream_pane.as_mut() else {
            return;
        };
        let Some(form) = pane.claim.take() else {
            return;
        };

        let consumer = form.consumer_input.read(cx).value().trim().to_string();
        if consumer.is_empty() {
            pane.claim = Some(form);
            return;
        }

        let group = form.group;
        self.focus_mode = super::KeyValueFocusMode::ValuePanel;

        let Some(connection) = self.get_connection(cx) else {
            self.report_connection_inactive(cx);
            return;
        };

        let (task_id, _cancel_token) = self.runner.start_mutation(
            TaskKind::KeyMutation,
            format!(
                "XCLAIM {} {group} {consumer}",
                dbflux_core::truncate_string_safe(&key, 40)
            ),
            cx,
        );

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;

                    let pending = api.stream_pending(&StreamPendingRequest {
                        key: key.clone(),
                        group: group.clone(),
                        keyspace,
                        count: PENDING_PAGE_SIZE,
                    })?;

                    api.stream_claim(&StreamClaimRequest {
                        key,
                        group,
                        consumer,
                        min_idle_ms: 0,
                        ids: pending.into_iter().map(|entry| entry.id).collect(),
                        keyspace,
                    })
                })
                .await;

            if let Err(error) = &result {
                report_error_async(
                    UserFacingError::new(ErrorKind::Driver, error.to_string()),
                    cx,
                );
            }

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    match result {
                        Ok(_) => this.runner.complete_mutation(task_id, cx),
                        Err(error) => this.runner.fail_mutation(task_id, error.to_string(), cx),
                    }

                    if let Some(pane) = this.stream_pane.as_mut() {
                        pane.pending = None;
                    }
                    this.load_stream_groups(cx);
                    cx.notify();
                });
            });
        })
        .detach();

        cx.notify();
    }
}

enum RangedRequest {
    ZSet(ZSetRangeRequest),
    Stream(StreamRangeRequest),
}

enum RangedPage {
    ZSet(dbflux_core::ZSetRangePage),
    Stream(dbflux_core::StreamRangePage),
}

/// Local time used by the stream table.
pub(super) fn local_now() -> DateTime<Local> {
    Local::now()
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_STREAM_FIELD_COLUMNS, busiest_group, entry_time_label, format_score,
        next_stream_bounds, relative_bar_fraction, short_entry_id, stream_end_bound,
        stream_entries_as_rows, stream_field_columns, stream_start_bound,
    };
    use chrono::{FixedOffset, TimeZone};
    use dbflux_core::{RangeOrder, StreamConsumerGroup, StreamEntry};

    fn entry(id: &str, fields: &[(&str, &str)]) -> StreamEntry {
        StreamEntry {
            id: id.to_string(),
            fields: fields
                .iter()
                .map(|(field, value)| (field.to_string(), value.to_string()))
                .collect(),
        }
    }

    #[test]
    fn bars_are_relative_to_the_top_score() {
        assert_eq!(relative_bar_fraction(4954.0, 4954.0, 4592.0), 1.0);
        assert!((relative_bar_fraction(4592.0, 4954.0, 4592.0) - 0.927).abs() < 0.001);
    }

    #[test]
    fn bars_spread_negative_scores_between_lowest_and_highest() {
        assert_eq!(relative_bar_fraction(-10.0, 10.0, -10.0), 0.0);
        assert_eq!(relative_bar_fraction(0.0, 10.0, -10.0), 0.5);
        assert_eq!(relative_bar_fraction(5.0, 5.0, 5.0), 1.0);
    }

    #[test]
    fn scores_group_integers_and_trim_decimals() {
        assert_eq!(format_score(4954.0), "4,954");
        assert_eq!(format_score(-1200.0), "-1,200");
        assert_eq!(format_score(0.25), "0.25");
        assert_eq!(format_score(2.345_678_9), "2.3457");
        assert_eq!(format_score(48_210.5), "48,210.5");
        assert_eq!(format_score(31_004.75), "31,004.75");
        assert_eq!(format_score(-12_345.678), "-12,345.678");
        assert_eq!(format_score(999.5), "999.5");
        assert_eq!(format_score(999.999_99), "1,000");
        assert_eq!(format_score(0.0), "0");
    }

    #[test]
    fn next_stream_page_continues_past_the_last_entry() {
        assert_eq!(
            next_stream_bounds(RangeOrder::Descending, "-", "+", None),
            ("-".to_string(), "+".to_string())
        );
        assert_eq!(
            next_stream_bounds(RangeOrder::Descending, "-", "+", Some("1790-0")),
            ("-".to_string(), "(1790-0".to_string())
        );
        assert_eq!(
            next_stream_bounds(RangeOrder::Ascending, "-", "+", Some("1790-0")),
            ("(1790-0".to_string(), "+".to_string())
        );
    }

    #[test]
    fn blank_stream_bounds_mean_the_whole_stream() {
        assert_eq!(stream_start_bound("  "), "-");
        assert_eq!(stream_end_bound(""), "+");
        assert_eq!(stream_start_bound(" 1790-0 "), "1790-0");
    }

    #[test]
    fn stream_columns_follow_first_appearance() {
        let entries = vec![
            entry("2-0", &[("order_id", "1"), ("status", "paid")]),
            entry(
                "1-0",
                &[("order_id", "2"), ("total", "9.5"), ("status", "x")],
            ),
        ];

        assert_eq!(
            stream_field_columns(&entries),
            vec!["order_id", "status", "total"]
        );
    }

    #[test]
    fn stream_columns_are_capped() {
        let fields: Vec<(String, String)> = (0..10)
            .map(|index| (format!("f{index}"), "v".to_string()))
            .collect();
        let entries = vec![StreamEntry {
            id: "1-0".to_string(),
            fields,
        }];

        assert_eq!(
            stream_field_columns(&entries).len(),
            MAX_STREAM_FIELD_COLUMNS
        );
    }

    #[test]
    fn entry_times_read_today_or_a_full_date() {
        let zone = FixedOffset::east_opt(0).expect("offset");
        let now = zone
            .with_ymd_and_hms(2026, 9, 23, 18, 0, 0)
            .single()
            .expect("valid time");

        let today_ms = zone
            .with_ymd_and_hms(2026, 9, 23, 17, 23, 20)
            .single()
            .expect("valid time")
            .timestamp_millis();
        let earlier_ms = zone
            .with_ymd_and_hms(2026, 9, 20, 8, 5, 1)
            .single()
            .expect("valid time")
            .timestamp_millis();

        assert_eq!(
            entry_time_label(&format!("{today_ms}-0"), &now).as_deref(),
            Some("today 17:23:20")
        );
        assert_eq!(
            entry_time_label(&format!("{earlier_ms}-3"), &now).as_deref(),
            Some("2026-09-20 08:05:01")
        );
        assert_eq!(entry_time_label("not-an-id", &now), None);
    }

    #[test]
    fn short_entry_ids_keep_the_tail() {
        assert_eq!(short_entry_id("1790194494866-0"), "…94866-0");
        assert_eq!(short_entry_id("1-0"), "1-0");
    }

    #[test]
    fn the_busiest_group_is_called_out() {
        let group = |name: &str, pending: u64| StreamConsumerGroup {
            name: name.to_string(),
            consumers: Vec::new(),
            pending,
            last_delivered_id: String::new(),
            oldest_pending_idle_ms: None,
        };

        let groups = vec![
            group("billing", 3),
            group("analytics", 0),
            group("mailer", 12),
        ];
        assert_eq!(
            busiest_group(&groups).map(|group| group.name.as_str()),
            Some("mailer")
        );
        assert!(busiest_group(&[group("idle", 0)]).is_none());
    }

    #[test]
    fn stream_rows_carry_ids_and_fields_for_the_member_editors() {
        let rows = stream_entries_as_rows(&[entry("5-0", &[("status", "paid")])]);

        assert_eq!(rows[0].entry_id.as_deref(), Some("5-0"));
        assert_eq!(rows[0].field.as_deref(), Some(r#"{"status":"paid"}"#));
    }
}
