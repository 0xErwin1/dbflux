/// Critical meaning of the telemetry copy, compared against the actual
/// source of every locale (not only "differs from English").
#[test]
fn telemetry_level_copy_keeps_the_critical_meaning_in_every_locale() {
    const WEEKLY: [&str; 4] = ["week", "semanal", "주간", "周报"];
    // The old, false claim that Basic sends the same as Off.
    const EQUIVALENCE_CLAIM: [&str; 4] = [
        "no more than Off",
        "no envía más que Apagado",
        "끔과 보내는 것이 같습니다",
        "「基本」与「关闭」发送的内容相同",
    ];
    const MINIMAL_PING: [&str; 4] = ["Ping", "Aviso", "핑", "通知"];
    const NOTHING_SENT: [&str; 4] = [
        "Nothing is sent",
        "No se envía nada",
        "아무것도 전송하지 않습니다",
        "不发送任何内容",
    ];
    const GET_STARTED: [&str; 4] = ["Get started", "Empezar", "시작하기", "开始使用"];

    for (index, locale) in ["en", "es", "ko", "zh_Hans"].iter().enumerate() {
        let basic = dbflux_i18n::t!(
            "updates.welcome.telemetry_basic_description",
            locale = locale
        );
        assert!(
            basic.contains(WEEKLY[index]),
            "{locale}: Basic must promise a weekly report, got: {basic}"
        );
        assert!(!basic.contains(EQUIVALENCE_CLAIM[index]));

        let off = dbflux_i18n::t!("updates.welcome.telemetry_off_description", locale = locale);
        assert!(
            off.contains(NOTHING_SENT[index]),
            "{locale}: Off must say that nothing is sent, got: {off}"
        );

        let minimal = dbflux_i18n::t!(
            "updates.welcome.telemetry_minimal_description",
            locale = locale
        );
        assert!(
            minimal.contains(MINIMAL_PING[index]),
            "{locale}: Minimal must describe the build ping, got: {minimal}"
        );

        let button = dbflux_i18n::t!("updates.welcome.get_started", locale = locale);
        assert_eq!(button, GET_STARTED[index]);
        let privacy = dbflux_i18n::t!("updates.welcome.telemetry_privacy", locale = locale);
        assert!(
            privacy.contains(&button),
            "{locale}: the privacy text must name the real button, got: {privacy}"
        );
    }
}
