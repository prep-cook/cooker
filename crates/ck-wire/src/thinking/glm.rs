//! Documented GLM model policies; aliases are resolved before these checks.
use crate::gostr::GoStr;

pub fn is_model(model: &str) -> bool {
    let model = model.strip_suffix("[1m]").unwrap_or(model);
    [
        "glm-5.3",
        "glm-5.3-flash",
        "glm-5.3-flashx",
        "glm-5.3-flashx-legal",
        "glm-5.2",
        "glm-5.1",
        "glm-5-turbo",
        "glm-5",
        "glm-4.7",
        "glm-4.7-flash",
        "glm-4.7-flashx",
        "glm-4.6",
        "glm-4.5-air",
        "glm-4.5-flash",
        "glm-4-flash-250414",
        "glm-4.6v",
        "glm-4.6v-flash",
    ]
    .iter()
    .any(|known| model.go_eq_fold(known))
}

pub fn strict_effort(model: &str) -> bool {
    let model = model.strip_suffix("[1m]").unwrap_or(model);
    ["glm-5.3", "glm-5.3-flash", "glm-5.3-flashx", "glm-5.3-flashx-legal"]
        .iter()
        .any(|known| model.go_eq_fold(known))
}

/// Effort levels a GLM model accepts when the operator configures none.
pub fn default_levels(model: &str) -> Vec<String> {
    let (model, _) = super::split_suffix(model);
    let model = model.strip_suffix("[1m]").unwrap_or(model);
    let levels: &[&str] = if strict_effort(model) {
        &["low", "high", "max"]
    } else if model.go_eq_fold("glm-5.2") {
        &["minimal", "low", "medium", "high", "xhigh", "max"]
    } else {
        &["low", "medium", "high"]
    };
    levels.iter().map(|&level| level.to_owned()).collect()
}
