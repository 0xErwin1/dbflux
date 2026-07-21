use super::context_menu::KvMenuTarget;
use super::parsing::parse_database_name;
use dbflux_core::{
    HashSetRequest, KeySetRequest, KeyType, ListEnd, ListPushRequest, MutationRequest,
    SetAddRequest, StreamAddRequest, StreamEntryId, ZSetAddRequest,
};
use gpui::*;

impl super::KeyValueDocument {
    pub(super) fn handle_copy_as_command(&self, target: KvMenuTarget, cx: &mut Context<Self>) {
        let Some(conn) = self.get_connection(cx) else {
            return;
        };
        let Some(generator) = conn.query_generator() else {
            return;
        };

        let keyspace = self.keyspace_index();
        let mutation = match target {
            KvMenuTarget::Key => self.build_key_mutation(keyspace),
            KvMenuTarget::Value => self.build_member_mutation(keyspace),
        };

        if let Some(mutation) = mutation
            && let Some(generated) = generator.generate_mutation(&mutation)
        {
            cx.write_to_clipboard(ClipboardItem::new_string(generated.text));
        }
    }

    fn build_key_mutation(&self, keyspace: Option<u32>) -> Option<MutationRequest> {
        let key = self.selected_key()?;
        let value = self.selected_value.as_ref()?;
        let key_type = value.entry.key_type?;

        match key_type {
            KeyType::String => {
                let mut request = KeySetRequest::new(key, value.value.clone());
                if let Some(ks) = keyspace {
                    request = request.with_keyspace(ks);
                }
                Some(MutationRequest::KeyValueSet(request))
            }

            KeyType::Hash => {
                let json = serde_json::from_slice::<serde_json::Value>(&value.value).ok()?;
                let serde_json::Value::Object(map) = json else {
                    return None;
                };

                let fields: Vec<(String, String)> = map
                    .into_iter()
                    .map(|(field, val)| {
                        let val_str = match val {
                            serde_json::Value::String(s) => s,
                            other => other.to_string(),
                        };
                        (field, val_str)
                    })
                    .collect();

                if fields.is_empty() {
                    return None;
                }

                Some(MutationRequest::KeyValueHashSet(HashSetRequest {
                    key,
                    fields,
                    keyspace,
                }))
            }

            KeyType::Set => {
                let json = serde_json::from_slice::<serde_json::Value>(&value.value).ok()?;
                let serde_json::Value::Array(items) = json else {
                    return None;
                };

                let members: Vec<String> = items
                    .into_iter()
                    .map(|item| match item {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    })
                    .collect();

                if members.is_empty() {
                    return None;
                }

                Some(MutationRequest::KeyValueSetAdd(SetAddRequest {
                    key,
                    members,
                    keyspace,
                }))
            }

            KeyType::List => {
                let json = serde_json::from_slice::<serde_json::Value>(&value.value).ok()?;
                let serde_json::Value::Array(items) = json else {
                    return None;
                };

                let values: Vec<String> = items
                    .into_iter()
                    .map(|item| match item {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    })
                    .collect();

                if values.is_empty() {
                    return None;
                }

                Some(MutationRequest::KeyValueListPush(ListPushRequest {
                    key,
                    values,
                    end: ListEnd::Tail,
                    keyspace,
                }))
            }

            KeyType::SortedSet => {
                let json = serde_json::from_slice::<serde_json::Value>(&value.value).ok()?;
                let serde_json::Value::Array(items) = json else {
                    return None;
                };

                let members: Vec<(String, f64)> = items
                    .into_iter()
                    .filter_map(|item| {
                        let obj = item.as_object()?;
                        let member = obj
                            .get("member")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())?;
                        let score = obj
                            .get("score")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        Some((member, score))
                    })
                    .collect();

                if members.is_empty() {
                    return None;
                }

                Some(MutationRequest::KeyValueZSetAdd(ZSetAddRequest {
                    key,
                    members,
                    keyspace,
                }))
            }

            KeyType::Stream | KeyType::Bytes | KeyType::Json | KeyType::Unknown => None,
        }
    }

    fn build_member_mutation(&self, keyspace: Option<u32>) -> Option<MutationRequest> {
        let key = self.selected_key()?;
        let key_type = self.selected_key_type()?;
        let member_idx = self.selected_member_index?;
        let member = self.cached_members.get(member_idx)?;

        match key_type {
            KeyType::Hash => {
                let field = member.field.clone()?;
                Some(MutationRequest::KeyValueHashSet(HashSetRequest {
                    key,
                    fields: vec![(field, member.display.clone())],
                    keyspace,
                }))
            }
            KeyType::Set => Some(MutationRequest::KeyValueSetAdd(SetAddRequest {
                key,
                members: vec![member.display.clone()],
                keyspace,
            })),
            KeyType::SortedSet => {
                let score = member.score.unwrap_or(0.0);
                Some(MutationRequest::KeyValueZSetAdd(ZSetAddRequest {
                    key,
                    members: vec![(member.display.clone(), score)],
                    keyspace,
                }))
            }
            KeyType::List => Some(MutationRequest::KeyValueListPush(ListPushRequest {
                key,
                values: vec![member.display.clone()],
                end: ListEnd::Tail,
                keyspace,
            })),
            KeyType::Stream => {
                let entry_id = member.entry_id.as_ref()?;
                let fields: Vec<(String, String)> = if let Some(field) = &member.field {
                    vec![(field.clone(), member.display.clone())]
                } else {
                    vec![("value".to_string(), member.display.clone())]
                };
                Some(MutationRequest::KeyValueStreamAdd(StreamAddRequest {
                    key,
                    id: StreamEntryId::Explicit(entry_id.clone()),
                    fields,
                    maxlen: None,
                    keyspace,
                }))
            }
            KeyType::String => {
                let value_bytes = self
                    .selected_value
                    .as_ref()
                    .map(|v| v.value.clone())
                    .unwrap_or_default();
                let mut request = KeySetRequest::new(key, value_bytes);
                if let Some(ks) = keyspace {
                    request = request.with_keyspace(ks);
                }
                Some(MutationRequest::KeyValueSet(request))
            }
            KeyType::Bytes | KeyType::Json | KeyType::Unknown => None,
        }
    }
}
