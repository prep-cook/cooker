use super::*;

pub(super) struct Names {
    custom: HashSet<Vec<u8>>,
    short: HashMap<Vec<u8>, Vec<u8>>,
}
impl Names {
    pub(super) fn new(raw: &[u8], tools: &[Res<'_>]) -> Self {
        let mut custom = HashSet::new();
        let mut functions = HashSet::new();
        for tool in tools {
            match tool.get("type").bytes().as_ref() {
                b"function" => {
                    functions.insert(tool.get("function.name").bytes().into_owned());
                }
                b"custom" => {
                    custom.insert(tool.get("name").bytes().into_owned());
                }
                _ => {}
            }
        }
        custom.retain(|n| !functions.contains(n));
        Self {
            custom,
            short: short_name_map(&request_tool_names(raw)),
        }
    }
    pub(super) fn short(&self, name: &[u8]) -> Vec<u8> {
        self.short.get(name).cloned().unwrap_or_else(|| shorten_name(name))
    }
    pub(super) fn custom(&self, name: &[u8]) -> bool {
        self.custom.contains(name)
    }
    fn resolve(&self, call: &Res<'_>) -> Option<(bool, Vec<u8>, Vec<u8>)> {
        match call.get("type").bytes().as_ref() {
            b"custom" => Some((
                true,
                call.get("custom.name").bytes().into_owned(),
                call.get("custom.input").bytes().into_owned(),
            )),
            b"function" => {
                let name = call.get("function.name").bytes().into_owned();
                let custom = self.custom(&name);
                let mut input = call.get("function.arguments").bytes().into_owned();
                if custom
                    && trim_space(&name) == b"apply_patch"
                    && let Some(unwrapped) = apply_patch::unwrap_input(&input)
                {
                    input = unwrapped.into_bytes();
                }
                Some((custom, name, input))
            }
            _ => None,
        }
    }
}

pub(super) fn convert(raw: &[u8], names: &Names) -> Result<Vec<Vec<u8>>, crate::Error> {
    let mut state = Input {
        names,
        items: Vec::new(),
        pending: Vec::new(),
        ambiguous: HashSet::new(),
        drops: UserTurnDrops::default(),
    };
    let messages = gj::get(raw, "messages");
    if messages.is_array() {
        for (i, message) in messages.array().iter().enumerate() {
            state.message(i, message);
        }
    }
    state.drops.result()?;
    Ok(state.items)
}

struct Input<'a> {
    names: &'a Names,
    items: Vec<Vec<u8>>,
    pending: Vec<PendingCall>,
    ambiguous: HashSet<Vec<u8>>,
    drops: UserTurnDrops,
}
impl Input<'_> {
    fn message(&mut self, index: usize, message: &Res<'_>) {
        let role = message.get("role").bytes().into_owned();
        if role == b"tool" {
            self.output(message);
            return;
        }
        self.pending.clear();
        self.ambiguous.clear();
        let mut msg = br#"{"type":"message"}"#.to_vec();
        gj::set_str(
            &mut msg,
            "role",
            if role == b"system" { &b"developer"[..] } else { &role },
        );
        let assistant = role == b"assistant";
        let text_type = if assistant { "output_text" } else { "input_text" };
        let user = role == b"user";
        let (parts, sendable) = self.parts(&message.get("content"), text_type, user);
        if user {
            self.drops.end_turn(sendable);
        }
        if !assistant || !parts.is_empty() {
            gj::set_raw(&mut msg, "content", gj::join(&parts));
            self.items.push(msg);
        }
        if assistant {
            self.calls(index, &message.get("tool_calls"));
        }
    }
    fn output(&mut self, message: &Res<'_>) {
        let id = message.get("tool_call_id").bytes().into_owned();
        if !id.is_empty() && self.ambiguous.contains(&id) {
            return;
        }
        let Some(call) = self
            .pending
            .iter_mut()
            .find(|c| !c.consumed && (id.is_empty() || c.source_id == id || c.call_id == id))
        else {
            return;
        };
        call.consumed = true;
        let mut output = b"{}".to_vec();
        gj::set_str(
            &mut output,
            "type",
            if call.custom {
                "custom_tool_call_output"
            } else {
                "function_call_output"
            },
        );
        gj::set_str(&mut output, "call_id", &call.call_id);
        set_tool_output(&mut output, &message.get("content"));
        self.items.push(output);
    }
    fn parts(&mut self, content: &Res<'_>, text_type: &str, user: bool) -> (Vec<Vec<u8>>, usize) {
        let mut parts = Vec::new();
        let mut sendable = 0;
        if content.kind == Kind::String && !content.s.is_empty() {
            let mut part = b"{}".to_vec();
            gj::set_str(&mut part, "type", text_type);
            gj::set_str(&mut part, "text", &content.s);
            parts.push(part);
            sendable += 1;
        } else if content.is_array() {
            for item in content.array() {
                if let Some((part, count)) = self.part(&item, text_type, user) {
                    parts.push(part);
                    sendable += count;
                }
            }
        }
        (parts, sendable)
    }
    fn part(&mut self, item: &Res<'_>, text_type: &str, user: bool) -> Option<(Vec<u8>, usize)> {
        let mut part = b"{}".to_vec();
        match item.get("type").bytes().as_ref() {
            b"text" => {
                gj::set_str(&mut part, "type", text_type);
                gj::set_str(&mut part, "text", item.get("text").bytes());
                return Some((part, usize::from(!item.get("text").bytes().is_empty())));
            }
            b"image_url" if user => {
                gj::set_str(&mut part, "type", "input_image");
                let url = item.get("image_url.url");
                if url.exists() {
                    gj::set_str(&mut part, "image_url", url.bytes());
                }
            }
            b"file" if user => match input_file_part(item) {
                Some(part) => return Some((part, 1)),
                None => {
                    self.drops.drop_part("file");
                    return None;
                }
            },
            b"input_audio" if user => {
                let data = item.get("input_audio.data").bytes();
                let format = item.get("input_audio.format").bytes();
                if data.is_empty() {
                    self.drops.drop_part("input_audio");
                    return None;
                }
                gj::set_str(&mut part, "type", "input_audio");
                gj::set_str(&mut part, "data", data);
                if !format.is_empty() {
                    gj::set_str(&mut part, "format", format);
                }
            }
            _ => return None,
        }
        Some((part, 1))
    }
    fn calls(&mut self, index: usize, calls: &Res<'_>) {
        if !calls.is_array() {
            return;
        }
        let calls = calls.array();
        let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut used = HashSet::new();
        for call in &calls {
            let id = call.get("id").bytes().into_owned();
            if self.names.resolve(call).is_some() && !id.is_empty() {
                *counts.entry(id.clone()).or_default() += 1;
                used.insert(id);
            }
        }
        self.ambiguous
            .extend(counts.into_iter().filter(|(_, n)| *n > 1).map(|(id, _)| id));
        for (j, call) in calls.iter().enumerate() {
            let Some((custom, name, input)) = self.names.resolve(call) else {
                continue;
            };
            let source_id = call.get("id").bytes().into_owned();
            if !source_id.is_empty() && self.ambiguous.contains(&source_id) {
                continue;
            }
            let call_id = unique_id(&source_id, index, j, &mut used);
            self.pending.push(PendingCall {
                call_id: call_id.clone(),
                source_id,
                custom,
                consumed: false,
            });
            let mut item = b"{}".to_vec();
            gj::set_str(
                &mut item,
                "type",
                if custom { "custom_tool_call" } else { "function_call" },
            );
            gj::set_str(&mut item, "call_id", &call_id);
            gj::set_str(&mut item, "name", self.names.short(&name));
            gj::set_str(&mut item, if custom { "input" } else { "arguments" }, &input);
            self.items.push(item);
        }
    }
}

fn unique_id(source: &[u8], i: usize, j: usize, used: &mut HashSet<Vec<u8>>) -> Vec<u8> {
    let mut id = source.to_vec();
    if id.is_empty() {
        let base = format!("call_missing_{i}_{j}").into_bytes();
        id = base.clone();
        let mut suffix = 1;
        while used.contains(&id) {
            id = [&base[..], format!("_{suffix}").as_bytes()].concat();
            suffix += 1;
        }
        used.insert(id.clone());
    }
    let mut shortened = shorten_call_id(&id);
    if shortened != id {
        use sha2::Digest;
        let prefix = sha2::Sha256::new_with_prefix(&id);
        let mut attempt = 1;
        while used.contains(&shortened) {
            let sum = prefix.clone().chain_update(format!("_{attempt}")).finalize();
            shortened = with_hash_suffix(&id, &sum);
            attempt += 1;
        }
        used.insert(shortened.clone());
        id = shortened;
    }
    id
}
