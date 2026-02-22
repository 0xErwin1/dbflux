use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RefreshPolicy {
    #[default]
    Manual,
    Interval {
        every_secs: u32,
    },
}

impl RefreshPolicy {
    pub const ALL: &'static [RefreshPolicy] = &[
        RefreshPolicy::Manual,
        RefreshPolicy::Interval { every_secs: 1 },
        RefreshPolicy::Interval { every_secs: 2 },
        RefreshPolicy::Interval { every_secs: 5 },
        RefreshPolicy::Interval { every_secs: 10 },
        RefreshPolicy::Interval { every_secs: 30 },
        RefreshPolicy::Interval { every_secs: 60 },
    ];

    pub fn every_secs(self) -> Option<u32> {
        match self {
            RefreshPolicy::Manual => None,
            RefreshPolicy::Interval { every_secs } => Some(every_secs),
        }
    }

    pub fn duration(self) -> Option<Duration> {
        self.every_secs()
            .map(|secs| Duration::from_secs(secs as u64))
    }

    pub fn is_auto(self) -> bool {
        matches!(self, RefreshPolicy::Interval { .. })
    }

    pub fn label(self) -> &'static str {
        match self {
            RefreshPolicy::Manual => "Off",
            RefreshPolicy::Interval { every_secs: 1 } => "1s",
            RefreshPolicy::Interval { every_secs: 2 } => "2s",
            RefreshPolicy::Interval { every_secs: 5 } => "5s",
            RefreshPolicy::Interval { every_secs: 10 } => "10s",
            RefreshPolicy::Interval { every_secs: 30 } => "30s",
            RefreshPolicy::Interval { every_secs: 60 } => "60s",
            RefreshPolicy::Interval { .. } => "Custom",
        }
    }

    pub fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|policy| *policy == self)
            .unwrap_or(0)
    }

    pub fn from_index(index: usize) -> Self {
        *Self::ALL.get(index).unwrap_or(&RefreshPolicy::Manual)
    }
}
