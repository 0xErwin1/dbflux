use super::SchemaNodeKind;

impl SchemaNodeKind {
    pub fn needs_click_handler(&self) -> bool {
        matches!(
            self,
            Self::Profile
                | Self::DatabasesFolder
                | Self::Database
                | Self::Table
                | Self::View
                | Self::Collection
                | Self::CollectionChild
                | Self::CollectionChildrenMore
                | Self::ConnectionFolder
                | Self::Schema
                | Self::TablesFolder
                | Self::ViewsFolder
                | Self::TypesFolder
                | Self::ColumnsFolder
                | Self::IndexesFolder
                | Self::ForeignKeysFolder
                | Self::ConstraintsFolder
                | Self::SchemaIndexesFolder
                | Self::SchemaForeignKeysFolder
                | Self::RoutinesFolder
                | Self::CollectionsFolder
                | Self::CollectionFieldsFolder
                | Self::CustomType
                | Self::ScriptsFolder
                | Self::ScriptFile
                | Self::DependentsFolder
                | Self::Routine
                | Self::MetricsFolder
                | Self::MetricNamespaceFolder
                | Self::MetricLeaf
                | Self::DashboardsFolder
                | Self::DashboardItem
                | Self::RemoteDashboardsFolder
                | Self::RemoteDashboardItem
                | Self::SavedChartsFolder
                | Self::SavedChartItem
                | Self::InstanceMetricsFolder
                | Self::InstanceMetricLeaf
                | Self::InstanceInspectorsFolder
                | Self::InstanceInspectorLeaf
                | Self::InstanceOverviewLeaf
        )
    }

    pub fn is_expandable_folder(&self) -> bool {
        matches!(
            self,
            Self::ConnectionFolder
                | Self::DatabasesFolder
                | Self::Schema
                | Self::TablesFolder
                | Self::ViewsFolder
                | Self::TypesFolder
                | Self::ColumnsFolder
                | Self::IndexesFolder
                | Self::ForeignKeysFolder
                | Self::ConstraintsFolder
                | Self::SchemaIndexesFolder
                | Self::SchemaForeignKeysFolder
                | Self::RoutinesFolder
                | Self::CollectionsFolder
                | Self::CollectionFieldsFolder
                | Self::Database
                | Self::CustomType
                | Self::ScriptsFolder
                | Self::DependentsFolder
                | Self::MetricsFolder
                | Self::MetricNamespaceFolder
                | Self::DashboardsFolder
                | Self::RemoteDashboardsFolder
                | Self::SavedChartsFolder
                | Self::InstanceMetricsFolder
                | Self::InstanceInspectorsFolder
        )
    }

    pub fn shows_pointer_cursor(&self) -> bool {
        matches!(
            self,
            Self::Profile
                | Self::Database
                | Self::ConnectionFolder
                | Self::CollectionChild
                | Self::CollectionChildrenMore
                | Self::ScriptFile
                | Self::Routine
                | Self::MetricLeaf
                | Self::DashboardItem
                | Self::RemoteDashboardItem
                | Self::SavedChartItem
                | Self::InstanceMetricLeaf
                | Self::InstanceInspectorLeaf
                | Self::InstanceOverviewLeaf
        )
    }
}
