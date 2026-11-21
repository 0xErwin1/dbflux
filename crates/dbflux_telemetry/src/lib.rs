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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Build {
    pub version: String,
    pub channel: Channel,
}

/// Consent level. `Off` sends nothing. `Minimal` sends one ping per build and
/// a weekly `active` signal. `Basic` sends the same ping and a weekly usage
/// report in place of the `active` signal. Persisted by variant name.
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
        require_string_fields(&value, &["schema", "decision"])?;
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
    // Decoded only to enforce the closed vocabularies.
    #[serde(rename = "schema")]
    _schema: EventSchema,
    event: EventKind,
    install_id: Uuid,
    delivery_id: Uuid,
    sent_at: DateTime<Utc>,
    version: String,
    channel: Channel,
    #[serde(rename = "os")]
    _os: Platform,
    #[serde(rename = "arch")]
    _arch: Architecture,
    system_language: Option<String>,
    country: Option<String>,
    features: Option<BTreeMap<Feature, u16>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum EventKind {
    Ping,
    Active,
    Basic,
}

// serde decodes a unit variant from `{"linux":null}` as well as from
// `"linux"`, so every wire enum must be checked to be a JSON string first.
fn require_string_fields(value: &serde_json::Value, fields: &[&str]) -> Result<(), ProtocolError> {
    let object = value.as_object().ok_or(ProtocolError)?;
    if fields
        .iter()
        .all(|field| object.get(*field).is_some_and(serde_json::Value::is_string))
    {
        Ok(())
    } else {
        Err(ProtocolError)
    }
}

struct DecodedEvent {
    wire: WireEvent,
    keys: usize,
}

/// The one strict decoder for a frozen delivery: duplicate keys, wire
/// identifier and time shapes, string enums and the closed field set. Callers
/// add their own identity and queue checks on top.
fn decode_event(json: &str) -> Result<DecodedEvent, ProtocolError> {
    let value = strict_json(json.as_bytes(), 4096)?;
    let install_id = wire_uuid(&value["install_id"])?;
    let delivery_id = wire_uuid(&value["delivery_id"])?;
    let sent_at = wire_time(&value["sent_at"])?;
    require_string_fields(&value, &["schema", "event", "channel", "os", "arch"])?;
    let keys = value.as_object().ok_or(ProtocolError)?.len();
    let wire: WireEvent = serde_json::from_value(value).map_err(|_| ProtocolError)?;
    if wire.install_id != install_id || wire.delivery_id != delivery_id || wire.sent_at != sent_at {
        return Err(ProtocolError);
    }
    Ok(DecodedEvent { wire, keys })
}

/// Checks the field set and the Basic dimensions that belong to the event kind.
fn validated_kind(decoded: &DecodedEvent) -> Result<EventKind, ProtocolError> {
    let wire = &decoded.wire;
    match wire.event {
        // Active carries exactly the ping shape: identity and platform only.
        EventKind::Ping | EventKind::Active if decoded.keys == 9 => Ok(wire.event),
        EventKind::Basic if decoded.keys == 12 => {
            let language = wire.system_language.as_ref().ok_or(ProtocolError)?;
            let country = wire.country.as_ref().ok_or(ProtocolError)?;
            let features = wire.features.as_ref().ok_or(ProtocolError)?;
            if normalize_system_language(language) != *language
                || country.len() != 2
                || !country.bytes().all(|byte| byte.is_ascii_uppercase())
                || features.values().any(|count| *count > 10_000)
            {
                return Err(ProtocolError);
            }
            Ok(EventKind::Basic)
        }
        _ => Err(ProtocolError),
    }
}

/// Validate the actual frozen bytes without reserializing or repairing them.
/// This is the canonical strict closed-schema check for one delivery payload
/// (duplicate keys rejected via the raw-byte scan); shared with read helpers
/// such as the audit-backed last-report lookup in `dbflux_app`.
pub fn validate_payload(json: &str) -> Result<(), ProtocolError> {
    let decoded = decode_event(json)?;
    if !valid_build(&Build {
        version: decoded.wire.version.clone(),
        channel: decoded.wire.channel,
    }) {
        return Err(ProtocolError);
    }
    validated_kind(&decoded).map(|_| ())
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
        let mut sent_builds = std::collections::HashSet::with_capacity(self.sent_builds.len());
        if !self
            .sent_builds
            .iter()
            .all(|build| sent_builds.insert(build))
        {
            return Err(ProtocolError);
        }
        let mut ping_builds = std::collections::HashSet::new();
        for delivery in &self.pending {
            if !delivery_ids.insert(delivery.id) {
                return Err(ProtocolError);
            }
            if !delivery.basic
                && !delivery.active
                && (sent_builds.contains(&delivery.build) || !ping_builds.insert(&delivery.build))
            {
                return Err(ProtocolError);
            }
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
            let decoded = decode_event(&delivery.json)?;
            let wire = &decoded.wire;
            if wire.install_id != self.install_id
                || wire.delivery_id != delivery.id
                || wire.version != delivery.build.version
                || wire.channel != delivery.build.channel
                || !valid_build(&delivery.build)
            {
                return Err(ProtocolError);
            }
            match (validated_kind(&decoded)?, delivery.basic, delivery.active) {
                (EventKind::Ping, false, false)
                | (EventKind::Active, false, true)
                | (EventKind::Basic, true, false) => {}
                _ => return Err(ProtocolError),
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
    /// Leaving Basic drops the queued report and the usage counters, and
    /// entering Basic drops a queued Minimal `active` event. Off also drops a
    /// queued ping, so nothing is left to send.
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
            Level::Basic => {
                // Basic replaces the Minimal weekly event, so an unsent one
                // must not go out beside the Basic report. Build pings stay.
                self.pending.retain(|delivery| !delivery.active);
            }
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
mod tests;
