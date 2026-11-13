//! The legend element for charts.
//!
//! The element moved to `bolt::chart::legend`; re-exported at this path so
//! call sites keep compiling. Its tooltip hint is asked by key from the
//! installed translator (`bolt::strings`), which this crate's theme adapter
//! answers from `dbflux_i18n`; the tests here pin that the catalogue answers
//! the two keys the legend asks for.

pub use bolt::chart::legend::legend_element;

#[cfg(test)]
mod tests {
    #[test]
    fn legend_keys_resolve_in_both_locales() {
        let en_hide = dbflux_i18n::t!("chart.legend.hide_series", locale = "en");
        let en_show = dbflux_i18n::t!("chart.legend.show_series", locale = "en");
        let es_hide = dbflux_i18n::t!("chart.legend.hide_series", locale = "es");
        let es_show = dbflux_i18n::t!("chart.legend.show_series", locale = "es");

        assert!(!en_hide.is_empty() && !en_hide.starts_with("en."));
        assert!(!en_show.is_empty() && !en_show.starts_with("en."));
        assert!(!es_hide.is_empty() && !es_hide.starts_with("es."));
        assert!(!es_show.is_empty() && !es_show.starts_with("es."));

        assert_ne!(
            dbflux_i18n::t!("chart.legend.hide_series", locale = "en"),
            dbflux_i18n::t!("chart.legend.hide_series", locale = "es"),
            "the hide hint is translated"
        );
    }
}
