use crate::item_manager::{DefaultFilename, ItemManager};
use crate::SshTunnelProfile;

pub type SshTunnelManager = ItemManager<SshTunnelProfile>;

impl DefaultFilename for SshTunnelManager {
    fn meta() -> (&'static str, &'static str) {
        ("ssh_tunnels.json", "SSH tunnel profiles")
    }
}
