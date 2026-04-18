use gpui::SharedString;

/// Source for an icon, supporting both named gpui_component icons and custom SVG paths.
#[derive(Clone)]
pub enum IconSource {
    /// A named icon from the gpui_component library.
    Named(gpui_component::IconName),
    /// A custom SVG asset path (e.g., from AppIcon::path()).
    Svg(SharedString),
}

impl std::fmt::Debug for IconSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IconSource::Named(_) => write!(f, "Named(..)"),
            IconSource::Svg(path) => f.debug_tuple("Svg").field(path).finish(),
        }
    }
}
