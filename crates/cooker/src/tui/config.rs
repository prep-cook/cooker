//! Editable runtime configuration through the admin API.
use std::sync::Arc;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use serde_json::Value;

use super::client::{Client, Object};
use super::i18n::t;
use super::style::{self as s, Doc, plain, span};
use super::widgets::{TextInput, Viewport};
use super::{Cmd, Msg, cmd};

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Kind {
    Bool,
    Int,
    Str,
    ReadOnly,
}

/// Go's `configField`.
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    pub label: &'static str,
    pub path: &'static str,
    pub kind: Kind,
    pub value: String,
}

/// Editable fields address the effective configuration tree.
pub fn parse_config(cfg: &Object) -> Vec<Field> {
    let fields = [
        ("Port", "server/port", Kind::ReadOnly),
        ("Host", "server/host", Kind::ReadOnly),
        ("Debug", "observability/logs/debug", Kind::Bool),
        ("Proxy URL", "requests/proxy-url", Kind::Str),
        ("Request Retry", "routing/retry/request-retry", Kind::Int),
        ("Max Retry Interval (s)", "routing/retry/max-retry-interval", Kind::Int),
        ("Force Model Prefix", "routing/force-model-prefix", Kind::Bool),
        ("Logging to File", "observability/logs/logging-to-file", Kind::Bool),
        (
            "Logs Max Total Size (MB)",
            "observability/logs/logs-max-total-size-mb",
            Kind::Int,
        ),
        (
            "Error Logs Max Files",
            "observability/logs/error-logs-max-files",
            Kind::Int,
        ),
        (
            "Usage Stats Enabled",
            "observability/usage/usage-statistics-enabled",
            Kind::Bool,
        ),
        ("Request Log", "observability/logs/request-log", Kind::Bool),
        ("Switch Project on Quota", "quota-exceeded/switch-project", Kind::Bool),
        (
            "Switch Preview Model",
            "quota-exceeded/switch-preview-model",
            Kind::Bool,
        ),
        ("Routing Strategy", "routing/strategy", Kind::Str),
    ];
    fields
        .into_iter()
        .map(|(label, path, kind)| {
            let value = super::client::value_at(cfg, path);
            let value = match kind {
                Kind::Bool => value.and_then(Value::as_bool).unwrap_or(false).to_string(),
                Kind::Int => value.and_then(Value::as_i64).unwrap_or(0).to_string(),
                _ if path == "routing/strategy" => value
                    .and_then(Value::as_str)
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or("soonest-reset")
                    .to_owned(),
                _ => value
                    .map(|v| v.as_str().map(str::to_owned).unwrap_or_else(|| v.to_string()))
                    .unwrap_or_default(),
            };
            Field {
                label,
                path,
                kind,
                value,
            }
        })
        .collect()
}

/// Go `fieldSection`.
fn section(path: &str) -> &'static str {
    if path.starts_with("quota-exceeded/") {
        return t("section_quota");
    }
    if path.starts_with("routing/") {
        return t("section_routing");
    }
    if path.starts_with("server/") || path.starts_with("requests/") {
        t("section_server")
    } else if path.starts_with("observability/") {
        t("section_logging")
    } else {
        t("section_other")
    }
}

/// Go `strconv.Atoi`: optional sign, ASCII digits, no spaces.
pub fn atoi(s: &str) -> Option<i64> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

pub struct ConfigTab {
    client: Arc<Client>,
    pub vp: Viewport,
    pub fields: Vec<Field>,
    pub cursor: usize,
    pub editing: bool,
    pub input: TextInput,
    err: Option<String>,
    message: Option<(String, Style)>,
}

impl ConfigTab {
    pub fn new(client: Arc<Client>) -> Self {
        ConfigTab {
            client,
            vp: Viewport::default(),
            fields: Vec::new(),
            cursor: 0,
            editing: false,
            input: TextInput::new(256),
            err: None,
            message: None,
        }
    }

    pub fn fetch(&self) -> Cmd {
        let client = self.client.clone();
        cmd(async move { Msg::ConfigData(client.get_config().await) })
    }

    pub fn set_size(&mut self, _width: usize, height: usize) {
        self.vp.height = height;
    }

    fn refresh(&mut self) {
        let doc = self.render();
        self.vp.set_content(doc);
    }

    pub fn locale(&mut self) {
        self.refresh();
    }

    pub fn data(&mut self, result: Result<Object, String>) {
        match result {
            Ok(cfg) => {
                self.err = None;
                self.fields = parse_config(&cfg);
            }
            Err(e) => {
                self.err = Some(e);
                self.fields.clear();
            }
        }
        self.refresh();
    }

    /// Go's `configUpdateMsg`: the result line, then a fresh read.
    pub fn updated(&mut self, err: Option<String>) -> Vec<Cmd> {
        self.message = Some(match err {
            Some(e) => (format!("✗ {e}"), s::error()),
            None => (t("updated_ok").to_owned(), s::success()),
        });
        self.refresh();
        vec![self.fetch()]
    }

    pub fn key(&mut self, key: &str) -> Vec<Cmd> {
        if self.editing {
            return self.editing_key(key);
        }
        match key {
            "r" => {
                self.message = None;
                return vec![self.fetch()];
            }
            "up" | "k" => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                    self.refresh();
                    self.ensure_cursor_visible();
                }
                return Vec::new();
            }
            "down" | "j" => {
                if self.cursor + 1 < self.fields.len() {
                    self.cursor += 1;
                    self.refresh();
                    self.ensure_cursor_visible();
                }
                return Vec::new();
            }
            "enter" | " " => {
                let Some(field) = self.fields.get(self.cursor).cloned() else {
                    return Vec::new();
                };
                return match field.kind {
                    Kind::ReadOnly => Vec::new(),
                    Kind::Bool => vec![self.put(field.path, Value::Bool(field.value != "true"))],
                    Kind::Int | Kind::Str => {
                        self.editing = true;
                        self.input.set_value(&field.value);
                        self.input.focus();
                        self.refresh();
                        Vec::new()
                    }
                };
            }
            _ => {}
        }
        self.vp.key(key);
        Vec::new()
    }

    fn editing_key(&mut self, key: &str) -> Vec<Cmd> {
        match key {
            "enter" => {
                self.editing = false;
                self.input.blur();
                let Some(field) = self.fields.get(self.cursor).cloned() else {
                    return Vec::new();
                };
                let raw = self.input.value();
                match field.kind {
                    Kind::Int => match atoi(&raw) {
                        Some(n) => vec![self.put(field.path, Value::from(n))],
                        None => {
                            let err = format!("{}: {raw}", t("invalid_int"));
                            let path = field.path.to_owned();
                            vec![cmd(async move {
                                Msg::ConfigUpdate {
                                    path,
                                    value: Value::Null,
                                    err: Some(err),
                                }
                            })]
                        }
                    },
                    _ => vec![self.put(field.path, Value::from(raw))],
                }
            }
            "esc" => {
                self.editing = false;
                self.input.blur();
                self.refresh();
                Vec::new()
            }
            _ => {
                self.input.key(key);
                self.refresh();
                Vec::new()
            }
        }
    }

    pub fn paste(&mut self, text: &str) {
        if self.editing {
            self.input.insert(text);
            self.refresh();
        }
    }

    fn put(&self, path: &'static str, value: Value) -> Cmd {
        let client = self.client.clone();
        cmd(async move {
            let err = client.put_field(path, value.clone()).await.err();
            Msg::ConfigUpdate {
                path: path.to_owned(),
                value,
                err,
            }
        })
    }

    /// Go `ensureCursorVisible`: the field sits about five lines below the top.
    fn ensure_cursor_visible(&mut self) {
        let target = self.cursor + 5;
        if target < self.vp.offset {
            self.vp.set_offset(target);
        }
        if target >= self.vp.offset + self.vp.height {
            self.vp.set_offset((target + 1).saturating_sub(self.vp.height));
        }
    }

    pub fn render_into(&self, area: Rect, buf: &mut Buffer) {
        self.vp.render(area, buf);
    }

    /// Go `renderContent`.
    fn render(&self) -> Doc {
        let mut doc = Doc::default();
        doc.title(t("config_title"));
        if let Some((message, style)) = &self.message {
            doc.line(vec![plain("  "), span(message.clone(), *style)]);
        }
        doc.text(t("config_help1"), s::help());
        doc.text(t("config_help2"), s::help());
        doc.blank();
        if let Some(err) = &self.err {
            doc.text(format!("  ⚠ Error: {err}"), s::error());
            return doc;
        }
        if self.fields.is_empty() {
            doc.text(t("no_config"), s::subtitle());
            return doc;
        }
        let mut current = "";
        for (i, f) in self.fields.iter().enumerate() {
            let sec = section(f.path);
            if sec != current {
                current = sec;
                doc.blank();
                doc.text(format!("  ── {sec} "), s::bold(s::HIGHLIGHT));
            }
            let selected = i == self.cursor;
            let mut label_style = s::fg(s::INFO);
            if selected {
                label_style = label_style.add_modifier(Modifier::BOLD);
            }
            let mut spans = vec![
                plain(if selected { "▸ " } else { "  " }),
                span(s::pad(f.label, 32), label_style),
                plain("  "),
            ];
            if self.editing && selected {
                spans.extend(self.input.view().spans);
            } else {
                spans.push(match f.kind {
                    Kind::Bool if f.value == "true" => span("● ON", s::success()),
                    Kind::Bool => span("○ OFF", s::fg(s::MUTED)),
                    Kind::ReadOnly => span(f.value.clone(), s::fg(s::SUBTEXT)),
                    _ => span(f.value.clone(), s::value()),
                });
            }
            let mut line = ratatui::text::Line::from(spans);
            if selected && !self.editing {
                line = line.style(Style::new().bg(s::SURFACE));
            }
            doc.0.push(line);
        }
        doc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_read_the_effective_tree() {
        let cfg = serde_json::json!({
            "server": {"port": 8317, "host": "127.0.0.1"},
            "observability": {"logs": {"debug": true}},
            "routing": {"retry": {"request-retry": 3}, "force-model-prefix": true}
        });
        let fields = parse_config(cfg.as_object().unwrap());
        let field = |path| fields.iter().find(|f| f.path == path).unwrap();
        assert_eq!(field("server/port").value, "8317");
        assert_eq!(field("observability/logs/debug").value, "true");
        assert_eq!(field("routing/retry/request-retry").value, "3");
        assert_eq!(field("routing/force-model-prefix").kind, Kind::Bool);
        assert_eq!(field("routing/force-model-prefix").value, "true");
        assert_eq!(field("routing/strategy").value, "soonest-reset");
        // Every field the tab writes is a setting the server accepts.
        for field in &fields {
            let value = match field.kind {
                Kind::ReadOnly => continue,
                Kind::Bool => "true",
                Kind::Int => "1",
                _ => "x",
            };
            let yaml = field
                .path
                .rsplit('/')
                .fold(value.to_owned(), |inner, key| format!("{{{key}: {inner}}}"));
            let doc: serde_yaml_ng::Value = serde_yaml_ng::from_str(&yaml).unwrap();
            ck_core::config::validate_config_fields(&doc, true)
                .unwrap_or_else(|e| panic!("{} is not a setting: {e}", field.path));
        }
    }

    #[test]
    fn atoi_follows_go() {
        assert_eq!(atoi("12"), Some(12));
        assert_eq!(atoi("-3"), Some(-3));
        assert_eq!(atoi("+4"), Some(4));
        for bad in ["", " 1", "1.0", "x", "-", "99999999999999999999"] {
            assert_eq!(atoi(bad), None, "{bad:?}");
        }
    }
}
