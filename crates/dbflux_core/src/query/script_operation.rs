use dbflux_policy::ExecutionClassification;

/// The database-level object a script statement targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptTarget {
    /// A database-level call, e.g. `db.dropDatabase()`.
    Database,
    /// A collection-level call, e.g. `db.<name>.find(...)`.
    Container(String),
}

/// The closed set of mongosh-style methods a script statement may invoke.
///
/// The JS method name typed by the user never crosses the engine/core
/// boundary as a string: [`ScriptMethod::from_js_name`] resolves it (or
/// rejects it) once, so [`ScriptMethod::classification`] is a total `match`
/// with no driver and no live database required to test it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptMethod {
    FindDocuments,
    AggregateDocuments,
    CountDocuments,
    InsertOne,
    InsertMany,
    UpdateOne,
    UpdateMany,
    ReplaceOne,
    DeleteOne,
    DeleteMany,
    DropContainer,
    DropDatabase,
    ListContainers,
    CreateContainer,
    DatabaseStats,
    ServerStatus,
    RunCommand,
}

impl ScriptMethod {
    /// Resolves a mongosh-style JS method name to a closed [`ScriptMethod`]
    /// variant. Returns `None` for anything not in the supported set, which
    /// the engine surfaces as a rejection naming the unresolved method.
    pub fn from_js_name(name: &str) -> Option<Self> {
        match name {
            "find" => Some(Self::FindDocuments),
            "aggregate" => Some(Self::AggregateDocuments),
            "countDocuments" | "count" | "estimatedDocumentCount" => Some(Self::CountDocuments),
            "insertOne" => Some(Self::InsertOne),
            "insertMany" => Some(Self::InsertMany),
            "updateOne" => Some(Self::UpdateOne),
            "updateMany" => Some(Self::UpdateMany),
            "replaceOne" => Some(Self::ReplaceOne),
            "deleteOne" => Some(Self::DeleteOne),
            "deleteMany" => Some(Self::DeleteMany),
            "drop" => Some(Self::DropContainer),
            "dropDatabase" => Some(Self::DropDatabase),
            "getCollectionNames" | "listCollections" => Some(Self::ListContainers),
            "createCollection" => Some(Self::CreateContainer),
            "stats" | "dbStats" => Some(Self::DatabaseStats),
            "serverStatus" => Some(Self::ServerStatus),
            "runCommand" => Some(Self::RunCommand),
            _ => None,
        }
    }

    /// The governance classification for this method, independent of the
    /// source text that produced it. Total match — every variant is covered.
    pub fn classification(self) -> ExecutionClassification {
        match self {
            Self::FindDocuments | Self::AggregateDocuments => ExecutionClassification::Read,
            Self::CountDocuments
            | Self::ListContainers
            | Self::DatabaseStats
            | Self::ServerStatus => ExecutionClassification::Metadata,
            Self::InsertOne | Self::InsertMany => ExecutionClassification::Write,
            Self::UpdateOne | Self::UpdateMany | Self::ReplaceOne => ExecutionClassification::Write,
            Self::DeleteOne | Self::DeleteMany => ExecutionClassification::Destructive,
            Self::DropContainer | Self::DropDatabase => ExecutionClassification::Destructive,
            Self::CreateContainer => ExecutionClassification::AdminSafe,
            // `runCommand` can carry an arbitrary admin command; the engine
            // cannot know its shape ahead of dispatch, so it is treated as
            // the worst case rather than guessed at.
            Self::RunCommand => ExecutionClassification::AdminDestructive,
        }
    }
}

/// A driver-agnostic descriptor for one dispatched script statement.
///
/// Constructed by the engine after resolving a JS method call; the driver's
/// [`ScriptOperationHost`] maps it onto its native operation type.
#[derive(Debug, Clone)]
pub struct ScriptOperation {
    pub target: ScriptTarget,
    pub method: ScriptMethod,
    pub arguments: Vec<serde_json::Value>,
}

impl ScriptOperation {
    pub fn new(
        target: ScriptTarget,
        method: ScriptMethod,
        arguments: Vec<serde_json::Value>,
    ) -> Self {
        Self {
            target,
            method,
            arguments,
        }
    }

    /// Classifies this constructed operation, independent of the source text
    /// that produced it (computed method names, loops, and conditionals all
    /// resolve to the same classification as a literal top-level call).
    pub fn classification(&self) -> ExecutionClassification {
        self.method.classification()
    }
}

/// Row-level effect counts returned by a dispatched write/destructive
/// operation. Fields that do not apply to a given method stay `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScriptOperationCounts {
    pub matched: Option<u64>,
    pub modified: Option<u64>,
    pub inserted: Option<u64>,
    pub deleted: Option<u64>,
    pub upserted: Option<u64>,
}

/// The result of dispatching one [`ScriptOperation`] through a
/// [`ScriptOperationHost`].
#[derive(Debug, Clone, Default)]
pub struct ScriptOperationOutcome {
    pub documents: Vec<serde_json::Value>,
    pub counts: ScriptOperationCounts,
}

/// Driver-implemented dispatch boundary for a single script statement.
///
/// The engine calls this once per statement it reaches during execution; the
/// driver is solely responsible for turning the closed [`ScriptOperation`]
/// descriptor into its native call.
pub trait ScriptOperationHost: Send {
    fn dispatch(&self, op: &ScriptOperation) -> Result<ScriptOperationOutcome, crate::DbError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== ScriptMethod::classification is total ====================

    #[test]
    fn find_and_aggregate_classify_as_read() {
        assert_eq!(
            ScriptMethod::FindDocuments.classification(),
            ExecutionClassification::Read
        );
        assert_eq!(
            ScriptMethod::AggregateDocuments.classification(),
            ExecutionClassification::Read
        );
    }

    #[test]
    fn count_and_introspection_classify_as_metadata() {
        for method in [
            ScriptMethod::CountDocuments,
            ScriptMethod::ListContainers,
            ScriptMethod::DatabaseStats,
            ScriptMethod::ServerStatus,
        ] {
            assert_eq!(
                method.classification(),
                ExecutionClassification::Metadata,
                "{method:?} must classify as Metadata"
            );
        }
    }

    #[test]
    fn inserts_and_updates_classify_as_write() {
        for method in [
            ScriptMethod::InsertOne,
            ScriptMethod::InsertMany,
            ScriptMethod::UpdateOne,
            ScriptMethod::UpdateMany,
            ScriptMethod::ReplaceOne,
        ] {
            assert_eq!(
                method.classification(),
                ExecutionClassification::Write,
                "{method:?} must classify as Write"
            );
        }
    }

    #[test]
    fn deletes_and_drops_classify_as_destructive() {
        for method in [
            ScriptMethod::DeleteOne,
            ScriptMethod::DeleteMany,
            ScriptMethod::DropContainer,
            ScriptMethod::DropDatabase,
        ] {
            assert_eq!(
                method.classification(),
                ExecutionClassification::Destructive,
                "{method:?} must classify as Destructive"
            );
        }
    }

    #[test]
    fn create_container_classifies_as_admin_safe() {
        assert_eq!(
            ScriptMethod::CreateContainer.classification(),
            ExecutionClassification::AdminSafe
        );
    }

    #[test]
    fn run_command_classifies_as_admin_destructive() {
        assert_eq!(
            ScriptMethod::RunCommand.classification(),
            ExecutionClassification::AdminDestructive
        );
    }

    // ==================== from_js_name resolution ====================

    #[test]
    fn from_js_name_resolves_every_known_method() {
        let known: &[(&str, ScriptMethod)] = &[
            ("find", ScriptMethod::FindDocuments),
            ("aggregate", ScriptMethod::AggregateDocuments),
            ("countDocuments", ScriptMethod::CountDocuments),
            ("count", ScriptMethod::CountDocuments),
            ("estimatedDocumentCount", ScriptMethod::CountDocuments),
            ("insertOne", ScriptMethod::InsertOne),
            ("insertMany", ScriptMethod::InsertMany),
            ("updateOne", ScriptMethod::UpdateOne),
            ("updateMany", ScriptMethod::UpdateMany),
            ("replaceOne", ScriptMethod::ReplaceOne),
            ("deleteOne", ScriptMethod::DeleteOne),
            ("deleteMany", ScriptMethod::DeleteMany),
            ("drop", ScriptMethod::DropContainer),
            ("dropDatabase", ScriptMethod::DropDatabase),
            ("getCollectionNames", ScriptMethod::ListContainers),
            ("listCollections", ScriptMethod::ListContainers),
            ("createCollection", ScriptMethod::CreateContainer),
            ("stats", ScriptMethod::DatabaseStats),
            ("dbStats", ScriptMethod::DatabaseStats),
            ("serverStatus", ScriptMethod::ServerStatus),
            ("runCommand", ScriptMethod::RunCommand),
        ];

        for (name, expected) in known {
            assert_eq!(
                ScriptMethod::from_js_name(name),
                Some(*expected),
                "{name} must resolve to {expected:?}"
            );
        }
    }

    #[test]
    fn from_js_name_rejects_unknown_methods() {
        assert_eq!(ScriptMethod::from_js_name("hasNext"), None);
        assert_eq!(ScriptMethod::from_js_name("limit"), None);
        assert_eq!(ScriptMethod::from_js_name("notAMethod"), None);
        assert_eq!(ScriptMethod::from_js_name(""), None);
    }

    // ==================== ScriptOperation::classification delegates ====================

    #[test]
    fn script_operation_classification_delegates_to_method() {
        let op = ScriptOperation::new(
            ScriptTarget::Container("users".to_string()),
            ScriptMethod::DeleteMany,
            vec![serde_json::json!({})],
        );
        assert_eq!(op.classification(), ExecutionClassification::Destructive);
    }
}
