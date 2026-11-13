//! What the open views of a secret tell each other. The listing and the
//! documents of single secrets each own a view of the same secrets, so a
//! delete, a create or a write in one is announced here for the others to
//! follow: a document learns its secret is gone, the listing learns what a
//! document wrote.

use gpui::{App, AppContext as _, Context, Entity, EntityId, EventEmitter, Global, Subscription};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SecretChangeKind {
    Created,
    /// A write or a follow-up read changed what the store holds for it.
    Changed,
    /// Its deletion was scheduled and confirmed by the store. Followers must
    /// not read from it until it is restored: the service refuses.
    Scheduled {
        date: Option<dbflux_core::chrono::DateTime<dbflux_core::chrono::Utc>>,
    },
    /// A scheduled deletion was cancelled and confirmed by the store.
    Restored,
    Removed,
}

/// One announcement. Names are region-scoped, so the region identifies the
/// secret together with the connection, the store and the name.
#[derive(Clone, Debug)]
pub(crate) struct SecretChange {
    pub profile_id: Uuid,
    pub store_id: String,
    pub region: Option<String>,
    pub name: String,
    pub kind: SecretChangeKind,
    /// The entity that announced it, which does not follow its own news.
    pub origin: EntityId,
}

/// Where a view's changes are announced, captured when a write starts so the
/// announcement does not depend on the view or its host still being around
/// when the write lands: the connection, the store, the region the write was
/// sent to, and the host whose own news it is.
#[derive(Clone, Debug)]
pub(crate) struct ChangeScope {
    pub profile_id: Uuid,
    pub store_id: String,
    pub region: Option<String>,
    pub origin: EntityId,
}

impl ChangeScope {
    /// Announces `kind` for `name` within this scope.
    pub(crate) fn publish(&self, name: &str, kind: SecretChangeKind, cx: &mut App) {
        publish(
            SecretChange {
                profile_id: self.profile_id,
                store_id: self.store_id.clone(),
                region: self.region.clone(),
                name: name.to_string(),
                kind,
                origin: self.origin,
            },
            cx,
        );
    }
}

impl SecretChange {
    /// Whether the announcement concerns the entries of this scope.
    pub(crate) fn is_in_scope(
        &self,
        profile_id: Uuid,
        store_id: &str,
        region: Option<&str>,
    ) -> bool {
        self.profile_id == profile_id
            && self.store_id == store_id
            && self.region.as_deref() == region
    }
}

struct ChangeHub;

impl EventEmitter<SecretChange> for ChangeHub {}

struct ChangeHubGlobal(Entity<ChangeHub>);

impl Global for ChangeHubGlobal {}

fn hub(cx: &mut App) -> Entity<ChangeHub> {
    if let Some(global) = cx.try_global::<ChangeHubGlobal>() {
        return global.0.clone();
    }

    let hub = cx.new(|_| ChangeHub);
    cx.set_global(ChangeHubGlobal(hub.clone()));
    hub
}

pub(crate) fn publish(change: SecretChange, cx: &mut App) {
    hub(cx).update(cx, |_, cx| cx.emit(change));
}

/// Follows the announcements of every other entity.
pub(crate) fn follow<T: 'static>(
    cx: &mut Context<T>,
    on_change: impl Fn(&mut T, &SecretChange, &mut Context<T>) + 'static,
) -> Subscription {
    let own = cx.entity_id();
    let hub = hub(cx);

    cx.subscribe(&hub, move |this, _, change: &SecretChange, cx| {
        if change.origin != own {
            on_change(this, change, cx);
        }
    })
}
