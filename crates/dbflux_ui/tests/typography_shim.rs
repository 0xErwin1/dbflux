use std::any::TypeId;

use dbflux_components::typography as shared_typography;
use dbflux_ui::ui::components::typography as ui_typography;

#[test]
fn ui_typography_re_exports_shared_typography_types() {
    assert_eq!(
        TypeId::of::<ui_typography::Headline>(),
        TypeId::of::<shared_typography::Headline>()
    );
    assert_eq!(
        TypeId::of::<ui_typography::SubSectionLabel>(),
        TypeId::of::<shared_typography::SubSectionLabel>()
    );
    assert_eq!(
        TypeId::of::<ui_typography::SidebarGroupLabel>(),
        TypeId::of::<shared_typography::SidebarGroupLabel>()
    );
    assert_eq!(
        TypeId::of::<ui_typography::KeyHint>(),
        TypeId::of::<shared_typography::KeyHint>()
    );
    assert_eq!(
        TypeId::of::<ui_typography::FieldLabel>(),
        TypeId::of::<shared_typography::FieldLabel>()
    );
    assert_eq!(
        TypeId::of::<ui_typography::RequiredMarker>(),
        TypeId::of::<shared_typography::RequiredMarker>()
    );
    assert_eq!(
        TypeId::of::<ui_typography::SectionDivider>(),
        TypeId::of::<shared_typography::SectionDivider>()
    );
}
