use super::{
    ClickElementArgs, ClickPointArgs, DragElementArgs, DragPointArgs, ElementArgs, GpuiMcp,
    InputCommand, Json, KeyArgs, MouseButton, NodeAction, Operation, Parameters, Point,
    PointerButtonArgs, PointerCommand, PointerMoveArgs, Role, ScrollArgs, ScrollPointArgs,
    SetTextArgs, SetValueArgs, ToolRouter, TypeTextArgs, Value, ack_json, encode_error, get_node,
    json, object_output, require_bounds, tool, tool_router, validate_pointer_point, validate_value,
};

#[tool_router(router = input_router)]
impl GpuiMcp {
    #[tool(
        description = "Return GPUI's current pointer position in window-relative logical pixels. This is also the position used by video cursor overlays."
    )]
    async fn pointer_location(&self) -> Result<Json<Value>, String> {
        let point = self.current_pointer_location().await?;
        Ok(object_output(json!({
            "x": point.x,
            "y": point.y,
            "coordinate_space": "window_logical_pixels",
        })))
    }

    #[tool(
        description = "Move GPUI's pointer to window-relative logical coordinates without moving the operating-system cursor."
    )]
    async fn pointer_move(
        &self,
        Parameters(args): Parameters<PointerMoveArgs>,
    ) -> Result<Json<Value>, String> {
        let point = Point {
            x: args.x,
            y: args.y,
        };
        validate_pointer_point(point)?;
        self.dispatch_pointer_input(PointerCommand::MouseMove {
            point,
            pressed_button: args.held_button,
        })
        .await?;
        Ok(ack_json("pointer_moved"))
    }

    #[tool(description = "Press a GPUI pointer button at window-relative logical coordinates.")]
    async fn pointer_down(
        &self,
        Parameters(args): Parameters<PointerButtonArgs>,
    ) -> Result<Json<Value>, String> {
        let point = Point {
            x: args.x,
            y: args.y,
        };
        validate_pointer_point(point)?;
        validate_pointer_click_count(args.count)?;
        self.dispatch_pointer_input(PointerCommand::MouseDown {
            point,
            button: args.button,
            click_count: args.count,
        })
        .await?;
        Ok(ack_json("pointer_down"))
    }

    #[tool(description = "Release a GPUI pointer button at window-relative logical coordinates.")]
    async fn pointer_up(
        &self,
        Parameters(args): Parameters<PointerButtonArgs>,
    ) -> Result<Json<Value>, String> {
        let point = Point {
            x: args.x,
            y: args.y,
        };
        validate_pointer_point(point)?;
        validate_pointer_click_count(args.count)?;
        self.dispatch_pointer_input(PointerCommand::MouseUp {
            point,
            button: args.button,
            click_count: args.count,
        })
        .await?;
        Ok(ack_json("pointer_up"))
    }

    #[tool(
        description = "Click through GPUI's real hit-testing and mouse-event pipeline at window-relative logical coordinates."
    )]
    async fn pointer_click(
        &self,
        Parameters(args): Parameters<PointerButtonArgs>,
    ) -> Result<Json<Value>, String> {
        self.click_at(
            Point {
                x: args.x,
                y: args.y,
            },
            args.button,
            args.count,
        )
        .await?;
        Ok(ack_json("pointer_clicked"))
    }

    #[tool(
        description = "Drag through GPUI's real hit-testing and drag/drop pipeline between logical coordinates."
    )]
    async fn pointer_drag(
        &self,
        Parameters(args): Parameters<DragPointArgs>,
    ) -> Result<Json<Value>, String> {
        self.drag_between(
            Point {
                x: args.from_x,
                y: args.from_y,
            },
            Point {
                x: args.to_x,
                y: args.to_y,
            },
            args.steps,
        )
        .await?;
        Ok(ack_json("pointer_dragged"))
    }

    #[tool(description = "Scroll through GPUI's real wheel-event pipeline at logical coordinates.")]
    async fn pointer_scroll(
        &self,
        Parameters(args): Parameters<ScrollPointArgs>,
    ) -> Result<Json<Value>, String> {
        self.scroll_at(
            Point {
                x: args.x,
                y: args.y,
            },
            args.delta_x,
            args.delta_y,
        )
        .await?;
        Ok(ack_json("pointer_scrolled"))
    }

    #[tool(
        description = "Click an element by stable semantic ID through GPUI's real hit-testing and mouse-event pipeline."
    )]
    async fn click_element(
        &self,
        Parameters(args): Parameters<ClickElementArgs>,
    ) -> Result<Json<Value>, String> {
        let point = self.element_point(&args.id, NodeAction::Click).await?;
        self.click_at(point, args.button, args.count).await?;
        Ok(ack_json("clicked"))
    }

    #[tool(
        description = "Double-click an element by stable semantic ID through GPUI's real input pipeline."
    )]
    async fn double_click_element(
        &self,
        Parameters(args): Parameters<ElementArgs>,
    ) -> Result<Json<Value>, String> {
        let point = self.element_point(&args.id, NodeAction::Click).await?;
        self.click_at(point, MouseButton::Left, 2).await?;
        Ok(ack_json("double_clicked"))
    }

    #[tool(description = "Click through GPUI's real input pipeline at logical coordinates.")]
    async fn click_coordinates(
        &self,
        Parameters(args): Parameters<ClickPointArgs>,
    ) -> Result<Json<Value>, String> {
        self.click_at(
            Point {
                x: args.x,
                y: args.y,
            },
            args.button,
            args.count,
        )
        .await?;
        Ok(ack_json("clicked"))
    }

    #[tool(
        description = "Move GPUI's pointer to an element's live bounds center so normal hover styles and handlers run."
    )]
    async fn hover_element(
        &self,
        Parameters(args): Parameters<ElementArgs>,
    ) -> Result<Json<Value>, String> {
        let point = self.element_point(&args.id, NodeAction::Hover).await?;
        self.dispatch_pointer_input(PointerCommand::MouseMove {
            point,
            pressed_button: None,
        })
        .await?;
        Ok(ack_json("hovered"))
    }

    #[tool(
        description = "Drag from one element to another through GPUI's real drag/drop pipeline."
    )]
    async fn drag_element(
        &self,
        Parameters(args): Parameters<DragElementArgs>,
    ) -> Result<Json<Value>, String> {
        let from = self.element_point(&args.from_id, NodeAction::Drag).await?;
        let tree = self.tree().await?;
        let to = require_bounds(get_node(&tree, &args.to_id)?)?.center();
        self.drag_between(from, to, args.steps).await?;
        Ok(ack_json("dragged"))
    }

    #[tool(
        description = "Drag through GPUI's real drag/drop pipeline between logical coordinates."
    )]
    async fn drag_coordinates(
        &self,
        Parameters(args): Parameters<DragPointArgs>,
    ) -> Result<Json<Value>, String> {
        self.drag_between(
            Point {
                x: args.from_x,
                y: args.from_y,
            },
            Point {
                x: args.to_x,
                y: args.to_y,
            },
            args.steps,
        )
        .await?;
        Ok(ack_json("dragged"))
    }

    #[tool(description = "Dispatch one cross-platform GPUI keystroke to the focused element.")]
    async fn keyboard(&self, Parameters(args): Parameters<KeyArgs>) -> Result<Json<Value>, String> {
        self.dispatch_input(InputCommand::Key {
            keystroke: args.keystroke,
        })
        .await?;
        Ok(ack_json("key_dispatched"))
    }

    #[tool(description = "Type UTF-8 text into the currently focused GPUI input.")]
    async fn type_text(
        &self,
        Parameters(args): Parameters<TypeTextArgs>,
    ) -> Result<Json<Value>, String> {
        self.dispatch_input(InputCommand::TypeText { text: args.text })
            .await?;
        Ok(ack_json("text_typed"))
    }

    #[tool(description = "Move keyboard focus to a focusable element by stable semantic ID.")]
    async fn focus_element(
        &self,
        Parameters(args): Parameters<ElementArgs>,
    ) -> Result<Json<Value>, String> {
        self.element_with_action(&args.id, NodeAction::Focus)
            .await?;
        self.ack_after_frame(Operation::Focus { node_id: args.id })
            .await?;
        Ok(ack_json("focused"))
    }

    #[tool(description = "Return text, caret, selection, and redaction metadata for an element.")]
    async fn get_text_info(
        &self,
        Parameters(args): Parameters<ElementArgs>,
    ) -> Result<Json<Value>, String> {
        let tree = self.tree().await?;
        let text = get_node(&tree, &args.id)?
            .text
            .as_ref()
            .ok_or_else(|| format!("element {:?} has no text", args.id))?;
        Ok(object_output(
            serde_json::to_value(text).map_err(encode_error)?,
        ))
    }

    #[tool(
        description = "Focus an editable element and replace its text through GPUI's active input handler."
    )]
    async fn set_text(
        &self,
        Parameters(args): Parameters<SetTextArgs>,
    ) -> Result<Json<Value>, String> {
        let node = self
            .element_with_action(&args.id, NodeAction::SetText)
            .await?;
        if node.text.as_ref().is_none_or(|text| text.redacted) {
            return Err("element is not an editable non-secret text field".to_owned());
        }
        self.ack_after_frame(Operation::Focus { node_id: args.id })
            .await?;
        self.dispatch_input(InputCommand::ReplaceText { text: args.text })
            .await?;
        Ok(ack_json("text_replaced"))
    }

    #[tool(description = "Return value metadata including numeric minimum, maximum, and step.")]
    async fn get_value(
        &self,
        Parameters(args): Parameters<ElementArgs>,
    ) -> Result<Json<Value>, String> {
        let tree = self.tree().await?;
        let value = get_node(&tree, &args.id)?
            .value
            .as_ref()
            .ok_or_else(|| format!("element {:?} has no value", args.id))?;
        Ok(object_output(
            serde_json::to_value(value).map_err(encode_error)?,
        ))
    }

    #[tool(
        description = "Set a value through the control's normal GPUI keyboard or click behavior."
    )]
    async fn set_value(
        &self,
        Parameters(args): Parameters<SetValueArgs>,
    ) -> Result<Json<Value>, String> {
        let tree = self.tree().await?;
        let node = get_node(&tree, &args.id)?.clone();

        match node.role {
            Role::TextInput | Role::SearchInput | Role::Combobox
                if node.actions.contains(&NodeAction::SetText)
                    || node.actions.contains(&NodeAction::SetValue) =>
            {
                let value = node
                    .value
                    .as_ref()
                    .ok_or_else(|| format!("element {:?} has no value", args.id))?;
                validate_value(&args.value, value)?;
                self.ack_after_frame(Operation::Focus { node_id: args.id })
                    .await?;
                self.dispatch_input(InputCommand::ReplaceText { text: args.value })
                    .await?;
            }
            Role::Checkbox | Role::Radio | Role::Switch => {
                let requested = parse_boolean(&args.value)?;
                let current = node.state.checked.ok_or_else(|| {
                    "checkable element did not expose its checked state".to_owned()
                })?;
                if requested != current {
                    let point = require_bounds(&node)?.center();
                    self.click_at(point, MouseButton::Left, 1).await?;
                }
            }
            Role::Slider => {
                let value = node
                    .value
                    .as_ref()
                    .ok_or_else(|| format!("element {:?} has no value", args.id))?;
                validate_value(&args.value, value)?;
                self.set_slider_value(&args.id, value, &args.value).await?;
            }
            _ => {
                return Err(format!(
                    "element {:?} does not expose a standard editable value behavior",
                    args.id
                ));
            }
        }
        Ok(ack_json("value_set"))
    }

    #[tool(description = "Count elements whose selected state is true.")]
    async fn get_selection_count(&self) -> Result<Json<Value>, String> {
        let tree = self.tree().await?;
        let count = tree
            .nodes
            .values()
            .filter(|node| node.state.selected == Some(true))
            .count();
        Ok(object_output(json!({ "selected_count": count })))
    }

    #[tool(
        description = "Return visible, enabled, focused, checked, selected, and expanded state for an element."
    )]
    async fn get_element_state(
        &self,
        Parameters(args): Parameters<ElementArgs>,
    ) -> Result<Json<Value>, String> {
        let tree = self.tree().await?;
        let state = &get_node(&tree, &args.id)?.state;
        Ok(object_output(
            serde_json::to_value(state).map_err(encode_error)?,
        ))
    }

    #[tool(
        description = "Scroll an element by semantic ID or scroll at window-relative logical coordinates through GPUI's real wheel-event pipeline."
    )]
    async fn scroll(
        &self,
        Parameters(args): Parameters<ScrollArgs>,
    ) -> Result<Json<Value>, String> {
        let point = match (&args.id, args.x, args.y) {
            (Some(id), None, None) => self.element_point(id, NodeAction::Scroll).await?,
            (None, Some(x), Some(y)) => Point { x, y },
            _ => return Err("provide either id or both x and y".to_owned()),
        };
        self.scroll_at(point, args.delta_x, args.delta_y).await?;
        Ok(ack_json("scrolled"))
    }

    async fn set_slider_value(
        &self,
        id: &str,
        current: &super::ValueInfo,
        requested: &str,
    ) -> Result<(), String> {
        let target = requested
            .parse::<f64>()
            .map_err(|_| "slider value must be numeric".to_owned())?;
        let min = current
            .min
            .ok_or_else(|| "slider must expose a minimum".to_owned())?;
        let step = current
            .step
            .filter(|step| *step > 0.0)
            .ok_or_else(|| "slider must expose a positive step".to_owned())?;
        let steps = ((target - min) / step).round();
        if !(0.0..=1_000.0).contains(&steps) {
            return Err("slider target requires more than 1000 keyboard steps".to_owned());
        }
        let step_count = (0..=1_000_u16)
            .find(|count| (f64::from(*count) - steps).abs() < f64::EPSILON)
            .ok_or_else(|| "slider target does not align to its step".to_owned())?;
        self.ack_after_frame(Operation::Focus {
            node_id: id.to_owned(),
        })
        .await?;
        let mut keystrokes = Vec::with_capacity(usize::from(step_count) + 1);
        keystrokes.push("home".to_owned());
        keystrokes.extend((0..step_count).map(|_| "right".to_owned()));
        self.dispatch_input(InputCommand::KeySequence { keystrokes })
            .await?;
        Ok(())
    }
}

pub(super) fn router() -> ToolRouter<GpuiMcp> {
    GpuiMcp::input_router()
}

fn validate_pointer_click_count(count: u8) -> Result<(), String> {
    (1..=3)
        .contains(&count)
        .then_some(())
        .ok_or_else(|| "pointer click count must be between 1 and 3".to_owned())
}

fn parse_boolean(value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "on" | "yes" => Ok(true),
        "false" | "0" | "off" | "no" => Ok(false),
        _ => Err("checkable values accept true or false".to_owned()),
    }
}
