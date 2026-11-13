use gpui::TestAppContext;

use super::{
    ACTIVITY_RAIL_KEY_CONTEXT, COMMAND_SEARCH_KEY_CONTEXT, ShellIcons, install_shell_icons,
};
use crate::icons::AppIcon;
use crate::key_contexts;

#[gpui::test]
fn the_title_bar_draws_the_product_icons(cx: &mut TestAppContext) {
    cx.update(|cx| {
        install_shell_icons(cx);

        let icons = ShellIcons::current(cx);
        assert_eq!(icons.search, AppIcon::Search.into());
        assert_eq!(icons.bell, AppIcon::Bell.into());
    });
}

#[test]
fn the_shell_key_contexts_are_the_ones_bolt_sets() {
    assert_eq!(key_contexts::ACTIVITY_RAIL, ACTIVITY_RAIL_KEY_CONTEXT);
    assert_eq!(key_contexts::COMMAND_SEARCH, COMMAND_SEARCH_KEY_CONTEXT);
}
