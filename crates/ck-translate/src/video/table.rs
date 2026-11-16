//! Video vocabulary: which options each action and dialect accepts, and the status words.

use super::{Action, Dialect};
use crate::codec::{Error, Result};

/// Internal statuses (`video.job`, INTERNAL-STREAM §4).
pub(super) const STATUSES: [&str; 6] = ["queued", "running", "completed", "failed", "cancelled", "expired"];

/// Wire word to internal status. Both dialects decode the same superset, so one
/// provider's wording never changes what the job means; only encoding is per dialect.
pub(super) const WIRE_STATUS: [(&str, &str); 14] = [
    ("queued", "queued"),
    ("pending", "queued"),
    ("in_progress", "running"),
    ("processing", "running"),
    ("running", "running"),
    ("completed", "completed"),
    ("done", "completed"),
    ("succeeded", "completed"),
    ("success", "completed"),
    ("failed", "failed"),
    ("error", "failed"),
    ("expired", "expired"),
    ("cancelled", "cancelled"),
    ("canceled", "cancelled"),
];

/// The word `dialect` uses for `status`. xAI says `pending|done|expired|failed`
/// (docs.x.ai `rest-api-reference/inference/videos.md`, GET `/v1/videos/{request_id}`). OpenAI's
/// reference lists `queued|in_progress|completed|failed`; `expired` and `cancelled` are cooker
/// extensions there, written so an OpenAI client sees the native state instead of a collapsed
/// `failed` (L17). A state a dialect has no word for at all (xAI `cancelled`) is refused.
pub(super) fn wire_status(dialect: Dialect, status: &str) -> Result<&'static str> {
    Ok(match (dialect, status) {
        (_, "failed") => "failed",
        (_, "expired") => "expired",
        (Dialect::OpenAi, "queued") => "queued",
        (Dialect::OpenAi, "running") => "in_progress",
        (Dialect::OpenAi, "completed") => "completed",
        (Dialect::OpenAi, "cancelled") => "cancelled",
        (Dialect::Xai, "queued" | "running") => "pending",
        (Dialect::Xai, "completed") => "done",
        (Dialect::Xai, "cancelled") => return Err(Error::Unsupported),
        _ => return Err(Error::Schema),
    })
}

/// Every option a request of `action` may carry in either dialect. Create, edit and extend share
/// one list so an option a dialect lacks (xAI `resolution` on an edit, OpenAI `size` on xAI) is
/// refused by [`accepts`] instead of turning into an unchecked native field.
pub(super) fn known(action: Action) -> &'static [&'static str] {
    match action {
        Action::Create | Action::Edit | Action::Extend => &[
            "model",
            "prompt",
            "seconds",
            "size",
            "duration",
            "resolution",
            "aspect_ratio",
            "input_reference",
            "image",
            "keyframes",
            "reference_images",
            "reference_audios",
            "video",
            "storage_options",
            "output",
            "user",
        ],
        Action::Retrieve => &[],
        Action::Content => &["variant"],
    }
}

/// Whether `dialect` can express option `name` of `action`. A known option the destination
/// lacks is refused at encode: dropping it would change the request without telling anyone.
///
/// OpenAI: developers.openai.com/api/reference/resources/videos/methods/{create,edit,extend}:
/// create `prompt input_reference model seconds size`, edit `prompt video`, extend
/// `prompt seconds video`. xAI: docs.x.ai `rest-api-reference/inference/videos.md`:
/// generations `aspect_ratio duration|seconds image keyframes model output prompt
/// reference_audios reference_images resolution storage_options user`, edits `model output
/// prompt storage_options user video`, extensions `duration model output prompt
/// storage_options video`.
pub(super) fn accepts(dialect: Dialect, action: Action, name: &str) -> bool {
    let list: &[&str] = match (dialect, action) {
        (Dialect::OpenAi, Action::Create) => &["model", "prompt", "seconds", "size", "input_reference"],
        (Dialect::OpenAi, Action::Edit) => &["prompt", "video"],
        (Dialect::OpenAi, Action::Extend) => &["prompt", "seconds", "video"],
        (Dialect::OpenAi, Action::Content) => &["variant"],
        (Dialect::Xai, Action::Create) => &[
            "model",
            "prompt",
            "image",
            "keyframes",
            "reference_images",
            "reference_audios",
            "duration",
            "seconds",
            "resolution",
            "aspect_ratio",
            "storage_options",
            "output",
            "user",
        ],
        (Dialect::Xai, Action::Edit) => &["model", "prompt", "video", "storage_options", "output", "user"],
        (Dialect::Xai, Action::Extend) => &["model", "prompt", "video", "duration", "storage_options", "output"],
        (_, Action::Retrieve) | (Dialect::Xai, Action::Content) => &[],
    };
    list.contains(&name)
}

#[derive(Clone, Copy)]
pub(super) enum Shape {
    Text,
    Count,
    Structure,
}

pub(super) fn shape(name: &str) -> Shape {
    match name {
        "model" | "prompt" | "size" | "resolution" | "aspect_ratio" | "variant" | "user" => Shape::Text,
        "seconds" | "duration" => Shape::Count,
        _ => Shape::Structure,
    }
}
