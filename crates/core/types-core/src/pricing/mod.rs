mod amount;
mod estimate;

use serde::{Deserialize, Serialize};

pub use amount::{UsdAmount, usd_number};
pub use estimate::{EstimatedUsdCost, ReportedCost, ServiceTier, estimate, estimate_for_model};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CostStatus {
    EstimatedFromUsage,

    ReportedByProvider,

    #[default]
    UsageNotReported,

    #[serde(other)]
    Other,
}

impl CostStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EstimatedFromUsage => "estimated_from_usage",
            Self::ReportedByProvider => "reported_by_provider",
            Self::UsageNotReported => "usage_not_reported",
            Self::Other => "other",
        }
    }
}
