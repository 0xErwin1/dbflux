use gpui::*;

struct HelloWorld {
    text: SharedString,
}

impl Render for HelloWorld {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
        .flex()
        .bg(rgb(0x2e7d32))
        .size_full()
        .justify_center()
        .items_center()
        .text_xl()
        .text_color(rgb(0xffffff))
        .child(format!("Hello, {}!", &self.text))
    }
}

fn main() {
    Application::new().run(|cx: &mut App| {
        cx.open_window(WindowOptions::default(), |_, cx| {
            cx.new(|_cx| HelloWorld {
                text: "World".into(),
            })
        })
        .unwrap();
    });
}