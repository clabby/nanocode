use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

const NANO_USD_PER_USD: u64 = 1_000_000_000;

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct UsdAmount {
    nano_usd: u64,
}

impl UsdAmount {
    #[must_use]
    pub const fn from_nano_usd(nano_usd: u64) -> Self {
        Self { nano_usd }
    }

    #[must_use]
    pub const fn nano_usd(self) -> u64 {
        self.nano_usd
    }

    #[must_use]
    pub fn decimal(self) -> String {
        format_decimal_usd(self.nano_usd)
    }

    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn as_f64(self) -> f64 {
        self.nano_usd as f64 / NANO_USD_PER_USD as f64
    }

    pub(super) const fn saturating_add(self, other: Self) -> Self {
        Self {
            nano_usd: self.nano_usd.saturating_add(other.nano_usd),
        }
    }

    pub(super) const fn saturating_mul(tokens: u64, nano_usd_per_token: u64) -> Self {
        Self {
            nano_usd: tokens.saturating_mul(nano_usd_per_token),
        }
    }
}

pub mod usd_number {
    use super::UsdAmount;
    use serde::{Deserialize, Deserializer, Serializer, de};

    #[allow(clippy::ref_option)]
    pub fn serialize<S>(value: &Option<UsdAmount>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(amount) => serializer.serialize_f64(amount.as_f64()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<UsdAmount>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<f64>::deserialize(deserializer)?
            .map(|usd| {
                if !usd.is_finite() || usd < 0.0 {
                    return Err(de::Error::custom("cost must be a non-negative USD amount"));
                }
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let nano_usd = (usd * 1e9).round() as u64;
                Ok(UsdAmount::from_nano_usd(nano_usd))
            })
            .transpose()
    }
}

impl fmt::Display for UsdAmount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "${}", self.decimal())
    }
}

impl Serialize for UsdAmount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.decimal())
    }
}

impl<'de> Deserialize<'de> for UsdAmount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_decimal_usd(&value)
            .map(Self::from_nano_usd)
            .map_err(de::Error::custom)
    }
}

fn parse_decimal_usd(value: &str) -> Result<u64, &'static str> {
    if value.is_empty() {
        return Err("USD amount must not be empty");
    }
    let (whole, fractional) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("expected a non-negative decimal USD amount");
    }
    if fractional.len() > 9 {
        return Err("USD amount has more than nine decimal places");
    }
    let fractional_digits =
        u32::try_from(fractional.len()).map_err(|_| "USD amount is too large")?;
    let whole = whole
        .parse::<u64>()
        .map_err(|_| "USD amount is too large")?;
    let fractional = if fractional.is_empty() {
        0
    } else {
        fractional
            .parse::<u64>()
            .map_err(|_| "USD amount is too large")?
            .checked_mul(10_u64.pow(9_u32.saturating_sub(fractional_digits)))
            .ok_or("USD amount is too large")?
    };
    whole
        .checked_mul(NANO_USD_PER_USD)
        .and_then(|whole| whole.checked_add(fractional))
        .ok_or("USD amount is too large")
}

fn format_decimal_usd(nano_usd: u64) -> String {
    let whole = nano_usd / NANO_USD_PER_USD;
    let fractional = nano_usd % NANO_USD_PER_USD;
    if fractional == 0 {
        return whole.to_string();
    }
    let fractional = format!("{fractional:09}");
    format!("{whole}.{}", fractional.trim_end_matches('0'))
}
