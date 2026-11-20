//! Bounded collector protocol and pure database-local scheduling state.

pub mod transport;

use chrono::{DateTime, Datelike, Duration, Utc};
use serde::{
    Deserialize, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    QueryRun,
    VisualQueryBuilder,
    DashboardOpen,
    ScriptRun,
    McpTool,
    SchemaInspect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Stable,
    Rc,
    Nightly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Darwin,
    Linux,
    Windows,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Architecture {
    Amd64,
    Arm64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Build {
    pub version: String,
    pub channel: Channel,
}

/// Consent level. `Off` sends nothing, `Minimal` sends one ping per build and
/// `Basic` adds the weekly usage report. Persisted by variant name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Level {
    Off,
    Minimal,
    Basic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid telemetry protocol")]
pub struct ProtocolError;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Acknowledgement {
    schema: ReceiptSchema,
    pub decision: Decision,
}
#[derive(Debug, Deserialize)]
enum ReceiptSchema {
    #[serde(rename = "dbflux.telemetry-delivery/v1")]
    V1,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Decision {
    Stored,
    Duplicate,
}

impl Acknowledgement {
    pub fn parse(status: u16, bytes: &[u8]) -> Result<Self, ProtocolError> {
        if status != 200 || bytes.len() > 1024 {
            return Err(ProtocolError);
        }
        let value = strict_json(bytes, 1024)?;
        let acknowledgement: Self = serde_json::from_value(value).map_err(|_| ProtocolError)?;
        match acknowledgement.schema {
            ReceiptSchema::V1 => Ok(acknowledgement),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum EventSchema {
    #[serde(rename = "dbflux.telemetry-event/v1")]
    V1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "event", rename_all = "lowercase")]
enum Payload {
    Ping,
    /// Minimal-level weekly signal: identity and platform only, which is why
    /// it is a separate kind rather than a `basic` without the dimensions.
    Active,
    Basic {
        system_language: String,
        country: String,
        features: BTreeMap<Feature, u16>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Event {
    schema: EventSchema,
    install_id: Uuid,
    delivery_id: Uuid,
    sent_at: DateTime<Utc>,
    version: String,
    channel: Channel,
    os: Platform,
    arch: Architecture,
    #[serde(flatten)]
    payload: Payload,
}

// serde's map decoding otherwise silently overwrites duplicate keys. Walk every
// object, including objects nested inside arrays, before typed field admission.
struct StrictValue(serde_json::Value);
impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("strict JSON")
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(StrictValue(value.into()))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(StrictValue(value.into()))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(StrictValue(value.into()))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(StrictValue(serde_json::Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(StrictValue(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(StrictValue(values.into()))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut object: A) -> Result<Self::Value, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some(key) = object.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                    let StrictValue(value) = object.next_value()?;
                    values.insert(key, value);
                }
                Ok(StrictValue(values.into()))
            }
        }
        deserializer.deserialize_any(StrictVisitor)
    }
}

// Payload numbers can occur only in feature counts. Normalize mathematical
// integers without floating point or allocations proportional to an exponent.
fn exact_count(token: &str) -> Result<u16, ProtocolError> {
    let (negative, unsigned) = token
        .strip_prefix('-')
        .map_or((false, token), |value| (true, value));
    let mut exponent_parts = unsigned.split(['e', 'E']);
    let coefficient = exponent_parts.next().ok_or(ProtocolError)?;
    let mut exponent = 0_i64;
    if let Some(raw) = exponent_parts.next() {
        let (negative, digits) = if let Some(digits) = raw.strip_prefix('-') {
            (true, digits)
        } else {
            (false, raw.strip_prefix('+').unwrap_or(raw))
        };
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ProtocolError);
        }
        for digit in digits.bytes() {
            exponent = (exponent * 10 + i64::from(digit - b'0')).min(8192);
        }
        if negative {
            exponent = -exponent;
        }
    }
    if exponent_parts.next().is_some() {
        return Err(ProtocolError);
    }
    let mut decimal_parts = coefficient.split('.');
    let integer = decimal_parts.next().ok_or(ProtocolError)?;
    if integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || (integer.len() > 1 && integer.starts_with('0'))
    {
        return Err(ProtocolError);
    }
    let fraction = decimal_parts.next();
    if fraction.is_some_and(|digits| {
        digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit())
    }) || decimal_parts.next().is_some()
    {
        return Err(ProtocolError);
    }
    let fraction = fraction.unwrap_or_default();
    let digits = format!("{integer}{fraction}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Ok(0);
    }
    if negative {
        return Err(ProtocolError);
    }
    let shift = exponent - fraction.len() as i64;
    let normalized = if shift < 0 {
        let removed = usize::try_from(-shift).map_err(|_| ProtocolError)?;
        let remaining = digits.len().checked_sub(removed).ok_or(ProtocolError)?;
        if !digits[remaining..].bytes().all(|byte| byte == b'0') {
            return Err(ProtocolError);
        }
        digits[..remaining].to_string()
    } else {
        if digits.len() as i64 + shift > 5 {
            return Err(ProtocolError);
        }
        format!("{digits}{}", "0".repeat(shift as usize))
    };
    let count: u16 = normalized.parse().map_err(|_| ProtocolError)?;
    if count > 10_000 {
        return Err(ProtocolError);
    }
    Ok(count)
}

fn strict_json(bytes: &[u8], limit: usize) -> Result<serde_json::Value, ProtocolError> {
    if bytes.len() > limit {
        return Err(ProtocolError);
    }
    let raw = std::str::from_utf8(bytes).map_err(|_| ProtocolError)?;
    // Keep string tokens untouched; normalize only unquoted numeric tokens in
    // this temporary parse buffer, never the persisted delivery body.
    let mut normalized = String::with_capacity(raw.len());
    let mut index = 0;
    let mut quoted = false;
    let mut escaped = false;
    while let Some(&byte) = bytes.get(index) {
        if quoted {
            let start = index;
            while let Some(&byte) = bytes.get(index) {
                index += 1;
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    quoted = false;
                    break;
                }
            }
            normalized.push_str(&raw[start..index]);
        } else if byte == b'"' {
            quoted = true;
            normalized.push('"');
            index += 1;
        } else if byte == b'-' || byte.is_ascii_digit() {
            let start = index;
            while bytes.get(index).is_some_and(|byte| {
                byte.is_ascii_digit() || matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E')
            }) {
                index += 1;
            }
            normalized.push_str(&exact_count(&raw[start..index])?.to_string());
        } else {
            // Non-ASCII bytes outside strings cannot be valid JSON.
            if !byte.is_ascii() {
                return Err(ProtocolError);
            }
            normalized.push(char::from(byte));
            index += 1;
        }
    }
    let StrictValue(value) = serde_json::from_str(&normalized).map_err(|_| ProtocolError)?;
    Ok(value)
}

fn wire_uuid(value: &serde_json::Value) -> Result<Uuid, ProtocolError> {
    let raw = value.as_str().ok_or(ProtocolError)?;
    if raw.len() != 36
        || !raw.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
    {
        return Err(ProtocolError);
    }
    let id = Uuid::parse_str(raw).map_err(|_| ProtocolError)?;
    if !valid_uuid(id) {
        return Err(ProtocolError);
    }
    Ok(id)
}

fn wire_time(value: &serde_json::Value) -> Result<DateTime<Utc>, ProtocolError> {
    let raw = value.as_str().ok_or(ProtocolError)?;
    let bytes = raw.as_bytes();
    if bytes.len() < 20
        || !bytes
            .iter()
            .take(19)
            .enumerate()
            .all(|(index, byte)| match index {
                4 | 7 => *byte == b'-',
                10 => *byte == b'T',
                13 | 16 => *byte == b':',
                _ => byte.is_ascii_digit(),
            })
        || bytes.get(17..19) == Some(b"60")
    {
        return Err(ProtocolError);
    }
    let mut index = 19;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let start = index;
        while bytes.get(index).is_some_and(|byte| byte.is_ascii_digit()) {
            index += 1;
        }
        if index == start {
            return Err(ProtocolError);
        }
    }
    let zone = bytes.get(index..).ok_or(ProtocolError)?;
    if zone != b"Z"
        && !(zone.len() == 6
            && matches!(zone[0], b'+' | b'-')
            && zone[3] == b':'
            && [zone[1], zone[2], zone[4], zone[5]]
                .iter()
                .all(u8::is_ascii_digit))
    {
        return Err(ProtocolError);
    }
    let time = DateTime::parse_from_rfc3339(raw)
        .map_err(|_| ProtocolError)?
        .with_timezone(&Utc);
    if time
        == DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z")
            .map_err(|_| ProtocolError)?
            .with_timezone(&Utc)
    {
        return Err(ProtocolError);
    }
    Ok(time)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEvent {
    schema: EventSchema,
    event: EventKind,
    install_id: Uuid,
    delivery_id: Uuid,
    sent_at: DateTime<Utc>,
    version: String,
    channel: Channel,
    os: Platform,
    arch: Architecture,
    system_language: Option<String>,
    country: Option<String>,
    features: Option<BTreeMap<Feature, u16>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum EventKind {
    Ping,
    Active,
    Basic,
}

/// Validate the actual frozen bytes without reserializing or repairing them.
/// This is the canonical strict closed-schema check for one delivery payload
/// (duplicate keys rejected via the raw-byte scan); shared with read helpers
/// such as the audit-backed last-report lookup in `dbflux_app`.
pub fn validate_payload(json: &str) -> Result<(), ProtocolError> {
    let value = strict_json(json.as_bytes(), 4096)?;
    wire_uuid(&value["install_id"])?;
    wire_uuid(&value["delivery_id"])?;
    wire_time(&value["sent_at"])?;
    let keys = value.as_object().ok_or(ProtocolError)?.len();
    let wire: WireEvent = serde_json::from_value(value).map_err(|_| ProtocolError)?;
    if !valid_build(&Build {
        version: wire.version,
        channel: wire.channel,
    }) {
        return Err(ProtocolError);
    }
    match wire.event {
        EventKind::Ping if keys == 9 => Ok(()),
        // Active carries exactly the ping shape: identity and platform only.
        EventKind::Active if keys == 9 => Ok(()),
        EventKind::Basic if keys == 12 => {
            let language = wire.system_language.ok_or(ProtocolError)?;
            let country = wire.country.ok_or(ProtocolError)?;
            let features = wire.features.ok_or(ProtocolError)?;
            if normalize_system_language(&language) != language
                || country.len() != 2
                || !country.bytes().all(|byte| byte.is_ascii_uppercase())
                || features.values().any(|count| *count > 10_000)
            {
                return Err(ProtocolError);
            }
            Ok(())
        }
        _ => Err(ProtocolError),
    }
}

fn valid_build(build: &Build) -> bool {
    build.version.len() <= 256 && semver::Version::parse(&build.version).is_ok()
}

fn valid_uuid(id: Uuid) -> bool {
    id.get_version() == Some(uuid::Version::Random) && id.get_variant() == uuid::Variant::RFC4122
}

pub fn normalize_system_language(locale: &str) -> String {
    let locale = locale
        .split(['.', '@'])
        .next()
        .unwrap_or_default()
        .replace('_', "-");
    let mut parts = locale.split('-');
    let primary = parts.next().unwrap_or_default().to_ascii_lowercase();
    if primary == "zh" {
        // An explicit script takes precedence over a region hint.
        for part in parts.clone() {
            match part.to_ascii_lowercase().as_str() {
                "hans" => return "zh-Hans".into(),
                "hant" => return "zh-Hant".into(),
                _ => {}
            }
        }
        for part in parts {
            match part.to_ascii_lowercase().as_str() {
                "cn" | "sg" => return "zh-Hans".into(),
                "tw" | "hk" | "mo" => return "zh-Hant".into(),
                _ => {}
            }
        }
    }
    if primary.len() == 2 && primary.bytes().all(|byte| byte.is_ascii_lowercase()) {
        primary
    } else {
        "und".into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingDelivery {
    id: Uuid,
    build: Build,
    basic: bool,
    /// A state file written before the `active` kind existed has no such key;
    /// those deliveries were pings or Basic heartbeats, so `false` is right.
    #[serde(default)]
    active: bool,
    json: String,
    retry_at: DateTime<Utc>,
    failures: u8,
}
impl PendingDelivery {
    /// Whether this delivery is the weekly `active` signal rather than a
    /// build ping or a Basic heartbeat. Hosts that assert per-build behaviour
    /// need it, because a Minimal queue holds both a ping and an active.
    pub fn is_active(&self) -> bool {
        self.active
    }
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn json(&self) -> &str {
        &self.json
    }
    pub fn ready(&self, now: DateTime<Utc>) -> bool {
        now >= self.retry_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryState {
    install_id: Uuid,
    /// Legacy field: the previous release rotated the id every 90 days and
    /// recorded when. It is still accepted on read so an existing state file
    /// loads, and never written again.
    #[serde(default, skip_serializing)]
    rotated_at: Option<DateTime<Utc>>,
    disclosure_dismissed: bool,
    level: Level,
    last_basic_success: Option<DateTime<Utc>>,
    #[serde(default)]
    last_active_success: Option<DateTime<Utc>>,
    sent_builds: Vec<Build>,
    counters: BTreeMap<Feature, u16>,
    pending: Vec<PendingDelivery>,
}
impl TelemetryState {
    pub fn new(_now: DateTime<Utc>) -> Self {
        Self {
            install_id: Uuid::new_v4(),
            rotated_at: None,
            disclosure_dismissed: false,
            level: Level::Basic,
            last_basic_success: None,
            last_active_success: None,
            sent_builds: Vec::new(),
            counters: BTreeMap::new(),
            pending: Vec::new(),
        }
    }
    pub fn install_id(&self) -> Uuid {
        self.install_id
    }
    pub fn pending(&self) -> &[PendingDelivery] {
        &self.pending
    }
    pub fn level(&self) -> Level {
        self.level
    }
    /// Persisted state must pass semantic checks before reuse or publication.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let mut delivery_ids = std::collections::BTreeSet::new();
        let mut ping_builds = Vec::new();
        for delivery in &self.pending {
            if !delivery_ids.insert(delivery.id) {
                return Err(ProtocolError);
            }
            if !delivery.basic && !delivery.active {
                if self.sent_builds.contains(&delivery.build)
                    || ping_builds.contains(&&delivery.build)
                {
                    return Err(ProtocolError);
                }
                ping_builds.push(&delivery.build);
            }
        }
        if self
            .sent_builds
            .iter()
            .enumerate()
            .any(|(index, build)| self.sent_builds[..index].contains(build))
        {
            return Err(ProtocolError);
        }
        if !valid_uuid(self.install_id)
            || self.counters.values().any(|count| *count > 10_000)
            || self.sent_builds.iter().any(|build| !valid_build(build))
            || (self.level != Level::Basic
                && (!self.counters.is_empty()
                    || self.pending.iter().any(|delivery| delivery.basic)))
            || (!self.disclosure_dismissed && !self.pending.is_empty())
            || self
                .pending
                .iter()
                .filter(|delivery| delivery.basic)
                .count()
                > 1
            || self
                .pending
                .iter()
                .filter(|delivery| delivery.active)
                .count()
                > 1
        {
            return Err(ProtocolError);
        }
        for delivery in &self.pending {
            if delivery.json.len() > 4096 || !valid_uuid(delivery.id) {
                return Err(ProtocolError);
            }
            let value = strict_json(delivery.json.as_bytes(), 4096)?;
            let install_id = wire_uuid(&value["install_id"])?;
            let delivery_id = wire_uuid(&value["delivery_id"])?;
            let sent_at = wire_time(&value["sent_at"])?;
            let keys = value.as_object().ok_or(ProtocolError)?.len();
            let wire: WireEvent = serde_json::from_value(value).map_err(|_| ProtocolError)?;
            if wire.install_id != self.install_id
                || wire.delivery_id != delivery.id
                || wire.version != delivery.build.version
                || wire.channel != delivery.build.channel
                || !valid_build(&delivery.build)
                || wire.install_id != install_id
                || wire.delivery_id != delivery_id
                || wire.sent_at != sent_at
            {
                return Err(ProtocolError);
            }
            match (wire.event, delivery.basic, delivery.active) {
                (EventKind::Ping, false, false) if keys == 9 => {}
                (EventKind::Active, false, true) if keys == 9 => {}
                (EventKind::Basic, true, false) if keys == 12 => {
                    let language = wire.system_language.ok_or(ProtocolError)?;
                    let country = wire.country.ok_or(ProtocolError)?;
                    let features = wire.features.ok_or(ProtocolError)?;
                    if normalize_system_language(&language) != language
                        || country.len() != 2
                        || !country.bytes().all(|byte| byte.is_ascii_uppercase())
                        || features.values().any(|count| *count > 10_000)
                    {
                        return Err(ProtocolError);
                    }
                }
                _ => return Err(ProtocolError),
            }
            // Decoding these fields enforces the closed schema/platform vocabularies.
            match (wire.schema, wire.os, wire.arch) {
                (EventSchema::V1, _, _) => {}
            }
        }
        Ok(())
    }
    /// Backoff changes only local scheduling metadata, never delivery content.
    pub fn retry_later(&mut self, id: Uuid, now: DateTime<Utc>) {
        if let Some(delivery) = self.pending.iter_mut().find(|delivery| delivery.id == id) {
            delivery.failures = delivery.failures.saturating_add(1);
            let seconds = (60_i64 << delivery.failures.min(10)).min(86_400);
            delivery.retry_at = now
                .checked_add_signed(Duration::seconds(seconds))
                .unwrap_or(DateTime::<Utc>::MAX_UTC);
        }
    }
    pub fn disclosure_dismissed(&self) -> bool {
        self.disclosure_dismissed
    }

    pub fn dismiss_disclosure(&mut self) {
        self.disclosure_dismissed = true;
    }
    /// Leaving Basic drops the queued report and the usage counters. Off also
    /// drops a queued ping, so nothing is left to send.
    pub fn set_level(&mut self, level: Level) {
        self.level = level;

        match level {
            Level::Off => {
                self.pending.clear();
                self.counters.clear();
            }
            Level::Minimal => {
                self.pending.retain(|delivery| !delivery.basic);
                self.counters.clear();
            }
            Level::Basic => {}
        }
    }
    /// Fixed-size foreground batch; callers persist the resulting state atomically.
    /// Usage is counted only once the user has seen the disclosure and kept
    /// Basic: Basic is preselected, so counting earlier would report activity
    /// from before the user agreed to it.
    fn counts_usage(&self) -> bool {
        self.disclosure_dismissed && self.level == Level::Basic
    }

    pub fn increment_batch(&mut self, counts: [(Feature, u16); 6]) {
        if !self.counts_usage() {
            return;
        }
        for (feature, amount) in counts {
            if amount != 0 {
                let count = self.counters.entry(feature).or_default();
                *count = count.saturating_add(amount).min(10_000);
            }
        }
    }

    pub fn increment(&mut self, feature: Feature) {
        if self.counts_usage() {
            let count = self.counters.entry(feature).or_default();
            *count = count.saturating_add(1).min(10_000);
        }
    }
    /// Reset mints a new identity and cancels every old-identity queue. Only
    /// a user asking for it, or a reset of local state, may call this: the id
    /// never changes on its own, because a rotating id cannot count unique
    /// installations across periods.
    pub fn reset_identity(&mut self, _now: DateTime<Utc>) {
        self.install_id = Uuid::new_v4();
        self.rotated_at = None;
        self.pending.clear();
        self.counters.clear();
    }
    /// Only a parsed, bounded acknowledgement can retire a delivery.
    pub fn acknowledge(
        &mut self,
        id: Uuid,
        _acknowledgement: &Acknowledgement,
        now: DateTime<Utc>,
    ) -> Result<(), ProtocolError> {
        self.validate()?;
        if let Some(index) = self.pending.iter().position(|delivery| delivery.id == id) {
            let delivery = self.pending.remove(index);
            if delivery.basic {
                self.last_basic_success = Some(now);
            } else if delivery.active {
                self.last_active_success = Some(now);
            } else if !self.sent_builds.contains(&delivery.build) {
                self.sent_builds.push(delivery.build);
            }
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub fn schedule(
        &mut self,
        now: DateTime<Utc>,
        build: &Build,
        os: Platform,
        arch: Architecture,
        locale: &str,
        country: Option<&str>,
    ) -> Result<(), ProtocolError> {
        self.validate()?;
        if !valid_build(build) || !(1..=9999).contains(&now.year()) {
            return Err(ProtocolError);
        }
        // Mutate a candidate so an encoding failure cannot consume snapshots.
        // The identity is stable: scheduling never re-mints it.
        let mut candidate = self.clone();
        if !candidate.disclosure_dismissed {
            *self = candidate;
            return Ok(());
        }
        if candidate.level == Level::Off {
            // A row stored while Off still queued a ping may carry one.
            candidate.pending.clear();
            *self = candidate;
            return Ok(());
        }
        if !candidate.sent_builds.contains(build)
            && !candidate
                .pending
                .iter()
                .any(|delivery| !delivery.basic && !delivery.active && &delivery.build == build)
        {
            candidate.queue(now, build, os, arch, Payload::Ping)?;
        }
        if candidate.level == Level::Minimal
            && !candidate.pending.iter().any(|delivery| delivery.active)
            && candidate
                .last_active_success
                .is_none_or(|last| now.signed_duration_since(last) >= Duration::days(7))
        {
            candidate.queue(now, build, os, arch, Payload::Active)?;
        }
        if candidate.level == Level::Basic
            && !candidate.pending.iter().any(|delivery| delivery.basic)
            && candidate
                .last_basic_success
                .is_none_or(|last| now.signed_duration_since(last) >= Duration::days(7))
        {
            let country = country
                .filter(|country| {
                    country.len() == 2 && country.bytes().all(|byte| byte.is_ascii_uppercase())
                })
                .unwrap_or("ZZ")
                .to_string();
            let features = std::mem::take(&mut candidate.counters);
            candidate.queue(
                now,
                build,
                os,
                arch,
                Payload::Basic {
                    system_language: normalize_system_language(locale),
                    country,
                    features,
                },
            )?;
        }
        *self = candidate;
        Ok(())
    }
    fn queue(
        &mut self,
        now: DateTime<Utc>,
        build: &Build,
        os: Platform,
        arch: Architecture,
        payload: Payload,
    ) -> Result<(), ProtocolError> {
        let id = Uuid::new_v4();
        let basic = matches!(payload, Payload::Basic { .. });
        let active = matches!(payload, Payload::Active);
        let event = Event {
            schema: EventSchema::V1,
            install_id: self.install_id,
            delivery_id: id,
            sent_at: now,
            version: build.version.clone(),
            channel: build.channel,
            os,
            arch,
            payload,
        };
        let json = serde_json::to_string(&event).map_err(|_| ProtocolError)?;
        let value = strict_json(json.as_bytes(), 4096)?;
        wire_uuid(&value["install_id"])?;
        wire_uuid(&value["delivery_id"])?;
        wire_time(&value["sent_at"])?;
        self.pending.push(PendingDelivery {
            id,
            build: build.clone(),
            basic,
            active,
            json,
            retry_at: now,
            failures: 0,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pings only: at Minimal the queue also holds a weekly `active`, so a
    /// per-build assertion must not count deliveries.
    fn pending_pings(state: &TelemetryState) -> usize {
        state
            .pending
            .iter()
            .filter(|delivery| !delivery.basic && !delivery.active)
            .count()
    }

    fn clock() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("fixture clock")
            .with_timezone(&Utc)
    }

    #[test]
    fn telemetry_language_normalization_is_bounded_and_script_first() {
        for (locale, expected) in [
            ("PT_br.UTF-8", "pt"),
            ("de-DE", "de"),
            ("C", "und"),
            ("english", "und"),
            ("", "und"),
            ("zh_TW", "zh-Hant"),
            ("zh_CN", "zh-Hans"),
            ("zh-TW-Hans", "zh-Hans"),
            ("zh", "zh"),
            ("ja_JP", "ja"),
        ] {
            assert_eq!(normalize_system_language(locale), expected, "{locale}");
        }
    }

    /// Minimal is the default level, so it must be able to report activity.
    /// It sends `active` weekly — never `basic`, which carries the opted-in
    /// dimensions — and Basic replaces it with the full heartbeat.
    #[test]
    fn minimal_sends_active_weekly_and_basic_replaces_it() {
        let now = clock();
        let build = Build {
            version: "1.5.0".into(),
            channel: Channel::Stable,
        };
        let mut state = TelemetryState::new(now);
        state.set_level(Level::Minimal);
        state.dismiss_disclosure();
        state
            .schedule(
                now,
                &build,
                Platform::Linux,
                Architecture::Amd64,
                "en",
                None,
            )
            .expect("schedule at Minimal");

        let kinds: Vec<&str> = state
            .pending
            .iter()
            .map(|delivery| {
                if delivery.basic {
                    "basic"
                } else if delivery.active {
                    "active"
                } else {
                    "ping"
                }
            })
            .collect();
        assert_eq!(
            kinds,
            vec!["ping", "active"],
            "Minimal sends ping plus active"
        );

        let active = state
            .pending
            .iter()
            .find(|delivery| delivery.active)
            .expect("active queued");
        let wire: serde_json::Value = serde_json::from_str(active.json()).expect("wire");
        assert_eq!(wire["event"], "active");
        assert_eq!(wire.as_object().expect("object").len(), 9);

        let ack = Acknowledgement::parse(
            200,
            br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
        )
        .expect("ack");
        let active_id = active.id;
        state
            .acknowledge(active_id, &ack, now + Duration::hours(1))
            .expect("acknowledge");
        state
            .schedule(
                now + Duration::days(1),
                &build,
                Platform::Linux,
                Architecture::Amd64,
                "en",
                None,
            )
            .expect("second schedule");
        assert!(
            !state.pending.iter().any(|delivery| delivery.active),
            "active must not repeat within the week"
        );

        let mut basic = TelemetryState::new(now);
        basic.dismiss_disclosure();
        basic.set_level(Level::Basic);
        basic
            .schedule(
                now,
                &build,
                Platform::Linux,
                Architecture::Amd64,
                "en",
                None,
            )
            .expect("schedule at Basic");
        assert!(
            basic.pending.iter().any(|delivery| delivery.basic),
            "Basic queues its heartbeat"
        );
        assert!(
            !basic.pending.iter().any(|delivery| delivery.active),
            "Basic must not also queue an active event"
        );
    }

    #[test]
    fn telemetry_minimal_ping_wire_and_rotation() {
        let now = clock();
        let mut state = TelemetryState::new(now);
        state.set_level(Level::Minimal);
        let build = Build {
            version: "1.2.3-rc.1+metadata".into(),
            channel: Channel::Rc,
        };
        assert_eq!(state.level(), Level::Minimal);
        state.increment(Feature::McpTool);
        state.dismiss_disclosure();
        state
            .schedule(
                now,
                &build,
                Platform::Darwin,
                Architecture::Arm64,
                "secret",
                Some("bad"),
            )
            .expect("queue");
        assert_eq!(pending_pings(&state), 1);
        let original = state.pending[0].clone();
        let json: serde_json::Value = serde_json::from_str(original.json()).expect("wire");
        let object = json.as_object().expect("object");
        assert_eq!(object.len(), 9);
        for key in [
            "schema",
            "event",
            "install_id",
            "delivery_id",
            "sent_at",
            "version",
            "channel",
            "os",
            "arch",
        ] {
            assert!(object.contains_key(key));
        }
        assert_eq!(json["schema"], "dbflux.telemetry-event/v1");
        assert_eq!(json["event"], "ping");
        assert_eq!(json["version"], build.version);
        assert_eq!(json["os"], "darwin");
        assert_eq!(json["arch"], "arm64");
        assert_eq!(json["sent_at"], "2026-01-01T00:00:00Z");
        assert_eq!(state.install_id.get_version(), Some(uuid::Version::Random));
        assert_eq!(original.id.get_version(), Some(uuid::Version::Random));
        state.retry_later(original.id, now);
        assert!(!state.pending[0].ready(now));
        assert!(state.pending[0].ready(now + Duration::days(1)));
        assert_eq!(state.pending[0].json(), original.json());
        // The installation id is stable for the life of the installation:
        // time passing never re-mints it, because a rotating id cannot count
        // unique installations across periods.
        let original_install = state.install_id();
        state
            .schedule(
                now + Duration::days(400),
                &build,
                Platform::Darwin,
                Architecture::Arm64,
                "en",
                None,
            )
            .expect("queue after more than a year");
        assert_eq!(
            state.install_id(),
            original_install,
            "the installation id must never rotate on its own"
        );
        assert_eq!(pending_pings(&state), 1);
        let ack = Acknowledgement::parse(
            200,
            br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
        )
        .expect("ack");
        state
            .acknowledge(state.pending[0].id, &ack, now + Duration::days(90))
            .expect("acknowledge valid state");
        state.reset_identity(now + Duration::days(91));
        state
            .schedule(
                now + Duration::days(91),
                &build,
                Platform::Darwin,
                Architecture::Arm64,
                "en",
                None,
            )
            .expect("acknowledged build");
        assert_eq!(
            pending_pings(&state),
            0,
            "an acknowledged build never pings again"
        );
        let other_build = Build {
            version: "1.2.3-rc.1+other".into(),
            channel: Channel::Rc,
        };
        state
            .schedule(
                now + Duration::days(91),
                &other_build,
                Platform::Linux,
                Architecture::Amd64,
                "en",
                None,
            )
            .expect("exact metadata build");
        assert_eq!(pending_pings(&state), 1, "a new build pings once");
    }

    fn fixture_build() -> Build {
        Build {
            version: "1.2.3".into(),
            channel: Channel::Stable,
        }
    }

    fn schedule_fixture(state: &mut TelemetryState, now: DateTime<Utc>) {
        state
            .schedule(
                now,
                &fixture_build(),
                Platform::Linux,
                Architecture::Amd64,
                "en_US",
                Some("US"),
            )
            .expect("schedule");
    }

    #[test]
    fn telemetry_off_queues_nothing_after_the_disclosure_is_dismissed() {
        let now = clock();
        let mut state = TelemetryState::new(now);
        state.dismiss_disclosure();
        state.set_level(Level::Off);

        state.increment(Feature::QueryRun);
        schedule_fixture(&mut state, now);
        schedule_fixture(&mut state, now + Duration::days(8));

        assert!(state.pending.is_empty());
        assert!(state.counters.is_empty());
        state.validate().expect("valid Off state");
    }

    #[test]
    fn telemetry_counts_nothing_before_the_disclosure_is_accepted() {
        let mut state = TelemetryState::new(clock());
        assert_eq!(state.level(), Level::Basic);

        state.increment(Feature::QueryRun);
        state.increment_batch([
            (Feature::QueryRun, 2),
            (Feature::VisualQueryBuilder, 2),
            (Feature::DashboardOpen, 2),
            (Feature::ScriptRun, 2),
            (Feature::McpTool, 2),
            (Feature::SchemaInspect, 2),
        ]);
        assert!(state.counters.is_empty());

        state.dismiss_disclosure();
        state.increment(Feature::QueryRun);
        assert_eq!(state.counters.get(&Feature::QueryRun), Some(&1));
    }

    #[test]
    fn telemetry_new_state_preselects_basic_without_sending() {
        let mut state = TelemetryState::new(clock());

        assert_eq!(state.level(), Level::Basic);
        assert!(!state.disclosure_dismissed());
        state.increment(Feature::McpTool);
        state
            .schedule(
                clock(),
                &Build {
                    version: "1.2.3".into(),
                    channel: Channel::Stable,
                },
                Platform::Linux,
                Architecture::Amd64,
                "en",
                None,
            )
            .expect("schedule before the disclosure");
        assert!(state.pending().is_empty());
        assert!(!state.disclosure_dismissed());
    }

    #[test]
    fn telemetry_minimal_queues_exactly_one_ping_per_build() {
        let now = clock();
        let mut state = TelemetryState::new(now);

        schedule_fixture(&mut state, now);
        assert!(state.pending.is_empty(), "nothing before the disclosure");

        state.dismiss_disclosure();
        state.increment(Feature::QueryRun);
        schedule_fixture(&mut state, now);
        schedule_fixture(&mut state, now + Duration::days(8));

        assert_eq!(pending_pings(&state), 1);
        assert!(!state.pending[0].basic);
        assert!(state.counters.is_empty(), "usage counting is Basic-only");
        let json: serde_json::Value = serde_json::from_str(state.pending[0].json()).expect("wire");
        assert_eq!(json["event"], "ping");
        assert_eq!(json.as_object().expect("object").len(), 9);

        let ack = Acknowledgement::parse(
            200,
            br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
        )
        .expect("ack");
        let id = state.pending[0].id;
        state.acknowledge(id, &ack, now).expect("acknowledge");
        schedule_fixture(&mut state, now + Duration::days(30));
        assert_eq!(pending_pings(&state), 0, "one ping per build");
    }

    #[test]
    fn telemetry_switching_basic_to_minimal_keeps_the_ping_and_drops_usage() {
        let now = clock();
        let mut state = TelemetryState::new(now);
        state.dismiss_disclosure();
        state.set_level(Level::Basic);
        state.increment(Feature::QueryRun);
        schedule_fixture(&mut state, now);
        state.increment(Feature::ScriptRun);

        state.set_level(Level::Minimal);

        assert_eq!(state.pending.len(), 1);
        assert!(!state.pending[0].basic);
        assert!(state.counters.is_empty());
        state.validate().expect("valid Minimal state");
    }

    #[test]
    fn telemetry_persisted_levels_keep_their_stored_names() {
        for (level, stored) in [
            (Level::Off, "Off"),
            (Level::Minimal, "Minimal"),
            (Level::Basic, "Basic"),
        ] {
            let mut state = TelemetryState::new(clock());
            state.set_level(level);
            let mut json = serde_json::to_value(&state).expect("state");
            assert_eq!(json["level"], stored);

            json["level"] = stored.into();
            let loaded: TelemetryState = serde_json::from_value(json).expect("stored row");
            assert_eq!(loaded.level(), level);
            loaded.validate().expect("valid stored row");
        }
    }

    #[test]
    fn telemetry_stored_off_row_with_a_queued_ping_loads_and_sends_nothing() {
        let now = clock();
        let mut state = TelemetryState::new(now);
        state.set_level(Level::Minimal);
        state.dismiss_disclosure();
        schedule_fixture(&mut state, now);
        assert_eq!(pending_pings(&state), 1);

        let mut json = serde_json::to_value(&state).expect("state");
        json["level"] = "Off".into();
        let mut loaded: TelemetryState = serde_json::from_value(json).expect("stored row");
        loaded
            .validate()
            .expect("row written before Off sent nothing");

        schedule_fixture(&mut loaded, now);
        assert!(loaded.pending.is_empty());
    }

    #[test]
    fn telemetry_switching_to_off_clears_a_pending_ping() {
        let now = clock();
        let mut state = TelemetryState::new(now);
        state.dismiss_disclosure();
        state.set_level(Level::Basic);
        state.increment(Feature::QueryRun);
        schedule_fixture(&mut state, now);
        assert_eq!(state.pending.len(), 2);

        state.set_level(Level::Off);

        assert!(state.pending.is_empty());
        assert!(state.counters.is_empty());
        schedule_fixture(&mut state, now);
        assert!(state.pending.is_empty());
    }

    #[test]
    fn telemetry_basic_snapshot_saturates_and_off_discards_prior_usage() {
        let now = clock();
        let mut state = TelemetryState::new(now);
        let build = Build {
            version: "1.2.3".into(),
            channel: Channel::Nightly,
        };
        state.dismiss_disclosure();
        state.set_level(Level::Basic);
        for feature in [
            Feature::QueryRun,
            Feature::VisualQueryBuilder,
            Feature::DashboardOpen,
            Feature::ScriptRun,
            Feature::McpTool,
            Feature::SchemaInspect,
        ] {
            for _ in 0..10_005 {
                state.increment(feature);
            }
        }
        state
            .schedule(
                now,
                &build,
                Platform::Windows,
                Architecture::Amd64,
                "zh_TW",
                None,
            )
            .expect("queue");
        let basic = state
            .pending
            .iter()
            .find(|delivery| delivery.basic)
            .expect("basic");
        let json: serde_json::Value = serde_json::from_str(basic.json()).expect("wire");
        assert_eq!(json.as_object().expect("object").len(), 12);
        assert_eq!(json["system_language"], "zh-Hant");
        assert_eq!(json["country"], "ZZ");
        let features = json["features"].as_object().expect("features");
        assert_eq!(features.len(), 6);
        assert!(features.values().all(|count| count == 10_000));
        let basic_id = basic.id;
        state.increment(Feature::QueryRun);
        let ack = Acknowledgement::parse(
            200,
            br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
        )
        .expect("ack");
        state
            .acknowledge(basic_id, &ack, now + Duration::days(2))
            .expect("acknowledge valid state");
        assert_eq!(state.counters.get(&Feature::QueryRun), Some(&1));
        state
            .schedule(
                now + Duration::days(8),
                &build,
                Platform::Windows,
                Architecture::Amd64,
                "en",
                None,
            )
            .expect("too early");
        assert!(!state.pending.iter().any(|delivery| delivery.basic));
        state
            .schedule(
                now + Duration::days(9),
                &build,
                Platform::Windows,
                Architecture::Amd64,
                "en",
                Some("US"),
            )
            .expect("weekly since success");
        state.set_level(Level::Minimal);
        assert_eq!(state.pending.len(), 1);
        assert!(!state.pending[0].basic);
        state.set_level(Level::Off);
        assert!(state.pending.is_empty());
        state.set_level(Level::Basic);
        state
            .schedule(
                now + Duration::days(10),
                &build,
                Platform::Windows,
                Architecture::Amd64,
                "en",
                None,
            )
            .expect("reenable");
        let json: serde_json::Value = serde_json::from_str(
            state
                .pending
                .iter()
                .find(|delivery| delivery.basic)
                .expect("basic")
                .json(),
        )
        .expect("wire");
        assert_eq!(json["features"], serde_json::json!({}));
        let previous = state.clone();
        let invalid = Build {
            version: "not a version".into(),
            channel: Channel::Stable,
        };
        assert!(
            state
                .schedule(
                    now,
                    &invalid,
                    Platform::Linux,
                    Architecture::Amd64,
                    "en",
                    None
                )
                .is_err()
        );
        assert_eq!(state, previous);
    }

    #[test]
    fn telemetry_batch_caps_all_six_and_respects_off() {
        let features = [
            Feature::QueryRun,
            Feature::VisualQueryBuilder,
            Feature::DashboardOpen,
            Feature::ScriptRun,
            Feature::McpTool,
            Feature::SchemaInspect,
        ];
        let batch = features.map(|feature| (feature, u16::MAX));
        let mut state = TelemetryState::new(clock());
        state.dismiss_disclosure();
        state.set_level(Level::Minimal);
        state.increment_batch(batch);
        assert!(state.counters.is_empty());
        state.set_level(Level::Basic);
        state.increment_batch(batch);
        assert_eq!(state.counters.len(), 6);
        assert!(state.counters.values().all(|count| *count == 10_000));
    }

    fn pending_basic() -> TelemetryState {
        let mut state = TelemetryState::new(clock());
        state.dismiss_disclosure();
        state.set_level(Level::Basic);
        state.increment(Feature::QueryRun);
        state
            .schedule(
                clock(),
                &Build {
                    version: "1.2.3".into(),
                    channel: Channel::Stable,
                },
                Platform::Linux,
                Architecture::Amd64,
                "en",
                None,
            )
            .expect("fixture queue");
        state
    }

    #[test]
    fn telemetry_persisted_payload_rejects_duplicate_feature_keys() {
        let mut state = pending_basic();
        let delivery = state
            .pending
            .iter_mut()
            .find(|delivery| delivery.basic)
            .expect("basic");
        delivery.json = delivery
            .json
            .replace("\"query_run\":1", "\"query_run\":1,\"query_run\":2");
        let decoded: WireEvent =
            serde_json::from_str(&delivery.json).expect("ordinary typed decode loses duplicates");
        assert_eq!(
            decoded.features.expect("features").get(&Feature::QueryRun),
            Some(&2)
        );
        let frozen = delivery.json.clone();
        assert!(
            state.validate().is_err(),
            "collector rejects duplicate nested keys"
        );
        assert_eq!(
            state
                .pending
                .iter()
                .find(|delivery| delivery.basic)
                .expect("basic")
                .json,
            frozen
        );
    }

    #[test]
    fn telemetry_strict_json_rejects_duplicates_at_every_depth() {
        for raw in [
            r#"{"a":1,"a":2}"#,
            r#"{"a":{"b":1,"b":2}}"#,
            r#"{"a":[{"b":1,"\u0062":2}]}"#,
            r#"{"a":1} {"b":2}"#,
        ] {
            assert_eq!(
                strict_json(raw.as_bytes(), 4096).expect_err("strict JSON"),
                ProtocolError
            );
        }
        assert!(
            strict_json(
                br#"{"a":[{"b":1},{"b":2}],"escaped":"\"query_run\":1,\"query_run\":2"}"#,
                4096
            )
            .is_ok()
        );
        assert!(strict_json(&[b' '; 4097], 4096).is_err());
    }

    #[test]
    fn telemetry_persisted_payload_contract_parity_and_immutability() {
        let state = pending_basic();
        let index = state
            .pending
            .iter()
            .position(|delivery| delivery.basic)
            .expect("basic");
        let original = &state.pending[index].json;
        let id = state.install_id().to_string();
        let malformed_id = id.replace('-', "");
        let variants = [
            original.replace(
                "\"schema\":",
                "\"schema\":\"dbflux.telemetry-event/v1\",\"schema\":",
            ),
            original.replace("\"query_run\":1", "\"query_run\":1,\"\\u0071uery_run\":2"),
            original.replace("\"query_run\":1", "\"unknown_feature\":1"),
            original.replace("\"os\":\"linux\"", "\"os\":\"linux\",\"extra\":null"),
            original.replace(&id, &malformed_id),
            original.replace(&id, &format!("urn:uuid:{id}")),
            original.replace(&id, &format!("{{{id}}}")),
            original.replace(&id, "00000000-0000-7000-8000-000000000000"),
            original.replace(&id, "00000000-0000-4000-0000-000000000000"),
            original.replace("2026-01-01T00:00:00Z", "2026-01-01 00:00:00Z"),
            original.replace("2026-01-01T00:00:00Z", "2026-01-01T00:00:60Z"),
            original.replace("2026-01-01T00:00:00Z", "2026-02-30T00:00:00Z"),
            original.replace("2026-01-01T00:00:00Z", "2026-01-01T00:00:00"),
            original.replace("2026-01-01T00:00:00Z", "0001-01-01T00:00:00Z"),
            original.replace("\"system_language\":\"en\"", "\"system_language\":null"),
            original.replace("\"features\":{\"query_run\":1}", "\"features\":null"),
        ];
        for raw in variants {
            let mut corrupt = state.clone();
            corrupt.pending[index].json = raw.clone();
            let error = corrupt
                .validate()
                .expect_err("out-of-contract frozen payload");
            assert_eq!(error.to_string(), "invalid telemetry protocol");
            assert_eq!(
                corrupt.pending[index].json, raw,
                "validation never repairs a frozen body"
            );
        }
        for time in [
            "2026-01-01T00:00:00.123Z",
            "2026-01-01T01:00:00+01:00",
            "2025-12-31T23:00:00-01:00",
        ] {
            let mut valid = state.clone();
            valid.pending[index].json = original
                .replace("2026-01-01T00:00:00Z", time)
                .replace(&id, &id.to_ascii_uppercase());
            let frozen = valid.pending[index].json.clone();
            valid
                .validate()
                .expect("collector UUID casing and RFC3339 offsets");
            assert_eq!(valid.pending[index].json, frozen);
        }
    }

    #[test]
    fn telemetry_feature_numbers_use_exact_bounded_decimal_arithmetic() {
        let state = pending_basic();
        let index = state
            .pending
            .iter()
            .position(|delivery| delivery.basic)
            .expect("basic");
        for (token, expected) in [
            ("12", true),
            ("12.0", true),
            ("1.2e1", true),
            ("10000.0", true),
            ("-0", true),
            ("0e99999999999999999999", true),
            ("0e-99999999999999999999", true),
            ("100000e-1", true),
            ("0.001e3", true),
            ("10000.000000000000001", false),
            ("10000000000000001e-12", false),
            ("10001", false),
            ("1.5", false),
            ("-1", false),
            ("1e400", false),
            ("1e-400", false),
            ("null", false),
            ("\"12\"", false),
            ("01", false),
        ] {
            let mut candidate = state.clone();
            candidate.pending[index].json = candidate.pending[index]
                .json
                .replace("\"query_run\":1", &format!("\"query_run\":{token}"));
            let frozen = candidate.pending[index].json.clone();
            assert_eq!(candidate.validate().is_ok(), expected, "{token}");
            assert_eq!(candidate.pending[index].json, frozen);
        }
    }

    #[test]
    fn telemetry_persisted_queue_rejects_conflicting_delivery_and_build_identities() {
        let state = pending_basic();
        let ping = state
            .pending
            .iter()
            .find(|delivery| !delivery.basic)
            .expect("ping")
            .clone();
        let mut duplicate_id = state.clone();
        duplicate_id.pending.push(ping.clone());
        let mut duplicate_build = state.clone();
        let mut other_ping = ping.clone();
        other_ping.id = Uuid::new_v4();
        other_ping.json = other_ping
            .json
            .replace(&ping.id.to_string(), &other_ping.id.to_string());
        duplicate_build.pending.push(other_ping);
        let mut shared_id = state.clone();
        let basic = shared_id
            .pending
            .iter_mut()
            .find(|delivery| delivery.basic)
            .expect("basic");
        basic.json = basic
            .json
            .replace(&basic.id.to_string(), &ping.id.to_string());
        basic.id = ping.id;
        let mut acknowledged_pending = state.clone();
        acknowledged_pending.sent_builds.push(ping.build.clone());
        let mut duplicate_history = state.clone();
        duplicate_history.sent_builds.extend(vec![
            Build {
                version: "1.0.0".into(),
                channel: Channel::Stable
            };
            2
        ]);
        let cases = [
            ("duplicate delivery", duplicate_id),
            ("duplicate ping build", duplicate_build),
            ("shared ping/basic identity", shared_id),
            ("acknowledged pending build", acknowledged_pending),
            ("duplicate sent history", duplicate_history),
        ];
        let observed: Vec<_> = cases
            .iter()
            .map(|(label, corrupt)| (*label, corrupt.validate().is_err()))
            .collect();
        let expected: Vec<_> = cases.iter().map(|(label, _)| (*label, true)).collect();
        assert_eq!(observed, expected);
        let ack = Acknowledgement::parse(
            200,
            br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
        )
        .expect("ack");
        for (_, mut corrupt) in cases {
            let before = corrupt.clone();
            assert!(corrupt.acknowledge(ping.id, &ack, clock()).is_err());
            assert_eq!(corrupt, before, "invalid ACK state must remain unchanged");
            assert!(
                corrupt
                    .schedule(
                        clock(),
                        &ping.build,
                        Platform::Linux,
                        Architecture::Amd64,
                        "en",
                        None
                    )
                    .is_err()
            );
            assert_eq!(
                corrupt, before,
                "invalid schedule state must remain unchanged"
            );
        }
    }

    #[test]
    fn telemetry_zero_time_cannot_be_queued() {
        let mut state = TelemetryState::new(clock());
        state.dismiss_disclosure();
        let before = state.clone();
        let zero = DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z")
            .expect("Go zero time")
            .with_timezone(&Utc);
        assert!(
            state
                .schedule(
                    zero,
                    &Build {
                        version: "1.2.3".into(),
                        channel: Channel::Stable
                    },
                    Platform::Linux,
                    Architecture::Amd64,
                    "en",
                    None
                )
                .is_err()
        );
        assert_eq!(state, before);
        state
            .schedule(
                zero + Duration::seconds(1),
                &Build {
                    version: "1.2.3".into(),
                    channel: Channel::Stable,
                },
                Platform::Linux,
                Architecture::Amd64,
                "en",
                None,
            )
            .expect("year one is valid outside the exact zero instant");
        state.validate().expect("valid year-one payload");
    }

    #[test]
    fn telemetry_persisted_state_rejects_out_of_contract_values() {
        let state = TelemetryState::new(clock());
        let mut json = serde_json::to_value(&state).expect("state");
        json["counters"] = serde_json::json!({"query_run": 10_001});
        let corrupt: TelemetryState = serde_json::from_value(json).expect("structural decode");
        assert!(corrupt.validate().is_err());
    }

    #[test]
    fn telemetry_ack_is_strict_and_bounded() {
        assert!(
            Acknowledgement::parse(
                200,
                br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#
            )
            .is_ok()
        );
        for bytes in [br#"{}"#.as_slice(), br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored","extra":1}"#, br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored","decision":"duplicate"}"#] {
            assert!(Acknowledgement::parse(200, bytes).is_err());
        }
        assert!(Acknowledgement::parse(200, &[b' '; 1025]).is_err());
        assert!(
            Acknowledgement::parse(
                503,
                br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#
            )
            .is_err()
        );
    }

    #[test]
    fn telemetry_schedule_preserves_snapshot_and_history() {
        let now = clock();
        let mut state = TelemetryState::new(now);
        let build = Build {
            version: "1.2.3+fixture".into(),
            channel: Channel::Stable,
        };
        state
            .schedule(
                now,
                &build,
                Platform::Linux,
                Architecture::Amd64,
                "en_US",
                None,
            )
            .expect("schedule");
        assert!(state.pending.is_empty());
        state.dismiss_disclosure();
        state.set_level(Level::Basic);
        state.increment(Feature::QueryRun);
        state
            .schedule(
                now,
                &build,
                Platform::Linux,
                Architecture::Amd64,
                "en_US",
                None,
            )
            .expect("schedule");
        assert_eq!(state.pending.len(), 2);
        let pending = state.pending.clone();
        state.increment(Feature::QueryRun);
        state
            .schedule(
                now,
                &build,
                Platform::Linux,
                Architecture::Amd64,
                "de",
                None,
            )
            .expect("retry");
        assert_eq!(state.pending, pending);
        assert_eq!(state.counters.get(&Feature::QueryRun), Some(&1));
        let ack = Acknowledgement::parse(
            200,
            br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"duplicate"}"#,
        )
        .expect("ack");
        for delivery in pending {
            state
                .acknowledge(delivery.id, &ack, now)
                .expect("acknowledge valid state");
        }
        state.reset_identity(now);
        state
            .schedule(
                now,
                &build,
                Platform::Linux,
                Architecture::Amd64,
                "en",
                None,
            )
            .expect("schedule");
        assert!(state.pending.is_empty());
        state
            .schedule(
                now + Duration::days(7),
                &build,
                Platform::Linux,
                Architecture::Amd64,
                "en",
                None,
            )
            .expect("weekly");
        assert_eq!(state.pending.len(), 1);
        state.set_level(Level::Off);
        assert!(state.pending.is_empty());
        assert!(state.counters.is_empty());
    }
}
