//! The write half of New secret. The dialog, its fields and what the listing
//! does with a new secret belong to the host; sending the request is a
//! governed write like every other, so it runs here, behind the same gates.

use super::Landing;
use crate::secret_store::SecretOperation;
use crate::secret_store::view::{SecretView, SecretViewEvent};
use crate::secret_store::write::create_secret;
use dbflux_core::{SecretCreateRequest, SecretCreated};
use gpui::Context;

impl SecretView {
    /// Sends `request` through the seam. Returns whether the write started:
    /// a gate that is not open (a region switch, an editor save, a missing
    /// connection) reports why and starts nothing.
    pub(in crate::secret_store) fn start_create(
        &mut self,
        request: SecretCreateRequest,
        cx: &mut Context<Self>,
    ) -> bool {
        let name = request.name.clone();

        let Some(session) = self.write_session(cx) else {
            return false;
        };

        self.run_governed_write(
            session,
            SecretOperation::Create,
            name.clone(),
            move |context| Landing::of(create_secret(context, &request)),
            move |view, landing, cx| view.land_create(&name, landing, cx),
            cx,
        );

        true
    }

    fn land_create(&mut self, name: &str, landing: Landing<SecretCreated>, cx: &mut Context<Self>) {
        match landing {
            Landing::Done(_) => {
                self.dismiss_dialog(cx);
                cx.emit(SecretViewEvent::Created {
                    name: name.to_string(),
                    stands: false,
                });
            }
            Landing::Stands(failure) => {
                self.dismiss_dialog(cx);
                self.report_write_failure(&failure, cx);
                cx.emit(SecretViewEvent::Created {
                    name: name.to_string(),
                    stands: true,
                });
            }
            Landing::Rejected(failure) => self.report_write_failure(&failure, cx),
        }
    }
}
