use super::*;
use crate::responses_policy::{Policy, meta_controls};
use ck_core::format::Format;

pub(crate) fn convert_with_policy(model: &str, raw: &[u8], policy: Policy) -> Vec<u8> {
    let mut out = br#"{"model":"","instructions":"","input":[]}"#.to_vec();
    let root = gj::parse(raw);
    let names = short_name_map(raw);
    gj::set_str(&mut out, "model", model);
    if policy.codex() {
        let tier = root.get("service_tier");
        if tier.kind == Kind::String && matches!(go_lower(trim_space(&tier.s)).as_slice(), b"priority" | b"fast") {
            gj::set_str(&mut out, "service_tier", "priority");
        }
    } else {
        meta_controls(&mut out, raw, Format::Gemini);
    }
    let mut input = Input {
        names: &names,
        items: Vec::new(),
        pending: Vec::new(),
        counter: 0,
    };
    input.system(&root);
    input.contents(&root);
    gj::set_items(&mut out, "input", &input.items);
    tools(&mut out, &root, &names);
    if policy.codex() {
        gj::set_bool(&mut out, "parallel_tool_calls", true);
    }
    set_tool_choice(&mut out, &root.get("toolConfig.functionCallingConfig"));
    if let Some(effort) = effort(&root).or_else(|| policy.codex().then(|| b"medium".to_vec())) {
        gj::set_str(&mut out, "reasoning.effort", effort);
    }
    gj::set_bool(&mut out, "stream", true);
    gj::set_bool(&mut out, "store", false);
    if policy.codex() {
        gj::set_strs(&mut out, "include", &["reasoning.encrypted_content"]);
    }
    for path in ck_wire::gemini_schema::walk(&gj::get(&out, "tools"), b"type") {
        let full = [&b"tools."[..], &path].concat();
        let value = gj::get(&out, &full);
        if value.kind == Kind::String {
            let lower = go_lower(&value.s);
            if lower != value.s.as_ref() {
                gj::set_str(&mut out, &full, &lower);
            }
        }
    }
    out
}

struct Input<'a> {
    names: &'a HashMap<Vec<u8>, Vec<u8>>,
    items: Vec<Vec<u8>>,
    pending: Vec<Vec<u8>>,
    counter: u64,
}
impl Input<'_> {
    fn next_id(&mut self) -> Vec<u8> {
        self.counter += 1;
        format!("call_gemini_{:016}", self.counter).into_bytes()
    }
    fn system(&mut self, root: &Res<'_>) {
        let system = first_of(root, ["system_instruction.parts", "systemInstruction.parts"]);
        if !system.is_array() {
            return;
        }
        let parts: Vec<Vec<u8>> = system
            .array()
            .iter()
            .filter(|p| !is_gemini_thought_part(p) && p.get("text").exists())
            .map(|p| typed_text("input_text", &p.get("text").bytes()))
            .collect();
        if !parts.is_empty() {
            let mut message = br#"{"type":"message","role":"developer","content":[]}"#.to_vec();
            gj::set_raw(&mut message, "content", gj::join(&parts));
            self.items.push(message);
        }
    }
    fn contents(&mut self, root: &Res<'_>) {
        let contents = root.get("contents");
        if !contents.is_array() {
            return;
        }
        for content in contents.array() {
            let mut role = content.get("role").bytes().into_owned();
            if role == b"model" {
                role = b"assistant".to_vec();
            }
            let parts = content.get("parts");
            if parts.is_array() {
                for part in parts.array() {
                    self.part(&role, &part);
                }
            }
        }
    }
    fn part(&mut self, role: &[u8], part: &Res<'_>) {
        if is_gemini_thought_part(part) {
            return;
        }
        let text = part.get("text");
        if text.exists() {
            let kind = if role == b"assistant" {
                "output_text"
            } else {
                "input_text"
            };
            self.items
                .push(message_with_part(role, &typed_text(kind, &text.bytes())));
            return;
        }
        if let Some(content) = inline_data_part(part).or_else(|| file_data_part(part)) {
            self.items.push(message_with_part(role, &content));
            return;
        }
        let call = part.get("functionCall");
        if call.exists() {
            self.call(&call);
            return;
        }
        let response = part.get("functionResponse");
        if response.exists() {
            self.response(&response);
        }
    }
    fn call(&mut self, call: &Res<'_>) {
        let mut item = br#"{"type":"function_call"}"#.to_vec();
        let name = call.get("name");
        if name.exists() {
            gj::set_str(&mut item, "name", mapped_name(self.names, &name.bytes()));
        }
        let args = call.get("args");
        if args.exists() {
            gj::set_str(&mut item, "arguments", &args.raw);
        }
        let mut id = call_id(call);
        if id.is_empty() {
            id = self.next_id();
        }
        gj::set_str(&mut item, "call_id", &id);
        self.pending.push(id);
        self.items.push(item);
    }
    fn response(&mut self, response: &Res<'_>) {
        let mut item = br#"{"type":"function_call_output"}"#.to_vec();
        let result = response.get("response.result");
        let body = response.get("response");
        if result.exists() {
            gj::set_str(&mut item, "output", result.bytes());
        } else if body.exists() {
            gj::set_str(&mut item, "output", &body.raw);
        }
        let custom = call_id(response);
        let id = if !custom.is_empty() {
            if let Some(position) = self.pending.iter().position(|p| *p == custom) {
                self.pending.remove(position);
            }
            custom
        } else if !self.pending.is_empty() {
            self.pending.remove(0)
        } else {
            self.next_id()
        };
        gj::set_str(&mut item, "call_id", id);
        self.items.push(item);
    }
}

fn tools(out: &mut Vec<u8>, root: &Res<'_>, names: &HashMap<Vec<u8>, Vec<u8>>) {
    let tools = root.get("tools");
    if !tools.is_array() {
        return;
    }
    let mut items = Vec::new();
    gj::set_str(out, "tool_choice", "auto");
    for tool in tools.array() {
        let declarations = tool.get("functionDeclarations");
        if !declarations.is_array() {
            continue;
        }
        for declaration in declarations.array() {
            let mut item = br#"{"type":"function"}"#.to_vec();
            let name = declaration.get("name");
            if name.exists() {
                gj::set_str(&mut item, "name", mapped_name(names, &name.bytes()));
            }
            let description = declaration.get("description");
            if description.exists() {
                gj::set_str(&mut item, "description", description.bytes());
            }
            let parameters = first_of(&declaration, ["parameters", "parametersJsonSchema"]);
            if parameters.exists() {
                gj::set_raw(&mut item, "parameters", clean_parameters(&parameters));
            }
            gj::set_bool(&mut item, "strict", false);
            items.push(item);
        }
    }
    gj::set_raw(out, "tools", gj::join(&items));
}

fn effort(root: &Res<'_>) -> Option<Vec<u8>> {
    let config = root.get("generationConfig");
    if !config.exists() {
        return None;
    }
    let level = first_of(&config, ["thinkingLevel", "thinking_level"]);
    let thinking = config.get("thinkingConfig");
    if level.exists() {
        return Some(effort_of(&level)).filter(|e| !e.is_empty());
    }
    if thinking.is_object() {
        let level = first_of(&thinking, ["thinkingLevel", "thinking_level"]);
        if level.exists() {
            return Some(effort_of(&level)).filter(|e| !e.is_empty());
        }
        let budget = first_of(&thinking, ["thinkingBudget", "thinking_budget"]);
        if budget.exists() {
            return ck_wire::thinking::convert_budget_to_level(budget.int()).map(|l| l.as_bytes().to_vec());
        }
    }
    None
}
