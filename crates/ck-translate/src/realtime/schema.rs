//! The documented control fields of Realtime client events, checked by the scan before a
//! message is forwarded. Source: the client-events reference
//! (<https://developers.openai.com/api/reference/resources/realtime/client-events>).
//!
//! Each [`Shape`] lists an object's documented members with their JSON kinds and child
//! shapes, the members every variant requires, and per-variant requirements keyed by the
//! object's `type` (and `role` for messages). Undocumented members are forwarded unchecked;
//! the upstream decides about them. Media and text members are checked by kind only.

/// JSON kinds a member may take.
pub(crate) const S: u8 = 1;
/// An integer: no fraction or exponent.
pub(crate) const I: u8 = 2;
/// Any number, integers included.
pub(crate) const N: u8 = 4;
pub(crate) const B: u8 = 8;
pub(crate) const O: u8 = 16;
pub(crate) const A: u8 = 32;
pub(crate) const Z: u8 = 64;
const ANY: u8 = S | N | B | O | A | Z;

/// One documented member, or the element of an array.
pub(crate) struct Member {
    pub(crate) name: &'static str,
    /// Dotted path for refusals.
    pub(crate) param: &'static str,
    pub(crate) kinds: u8,
    pub(crate) object: Option<&'static Shape>,
    pub(crate) items: Option<&'static Member>,
}

const fn m(name: &'static str, param: &'static str, kinds: u8) -> Member {
    Member {
        name,
        param,
        kinds,
        object: None,
        items: None,
    }
}
const fn obj(name: &'static str, param: &'static str, kinds: u8, shape: &'static Shape) -> Member {
    Member {
        object: Some(shape),
        ..m(name, param, kinds)
    }
}
const fn arr(name: &'static str, param: &'static str, kinds: u8, items: &'static Member) -> Member {
    Member {
        items: Some(items),
        ..m(name, param, kinds)
    }
}

/// One variant of an object, selected by its `type`; `None` matches an absent `type`.
pub(crate) struct Variant {
    pub(crate) ty: Option<&'static str>,
    /// Allowed `role` values; empty when the variant has no role.
    pub(crate) roles: &'static [&'static str],
    pub(crate) required: &'static [&'static str],
}

const fn v(ty: &'static str, required: &'static [&'static str]) -> Variant {
    Variant {
        ty: Some(ty),
        roles: &[],
        required,
    }
}
const UNTYPED: Variant = Variant {
    ty: None,
    roles: &[],
    required: &[],
};

pub(crate) struct Shape {
    pub(crate) members: &'static [Member],
    pub(crate) required: &'static [&'static str],
    /// Empty: any `type`. Otherwise `type` must select one of these.
    pub(crate) variants: &'static [Variant],
}

impl Shape {
    pub(crate) fn member(&self, key: &[u8]) -> Option<(usize, &'static Member)> {
        self.members
            .iter()
            .position(|m| m.name.as_bytes() == key)
            .map(|index| (index, &self.members[index]))
    }
    pub(crate) fn param(&self, name: &str) -> &'static str {
        self.members.iter().find(|m| m.name == name).map_or("", |m| m.param)
    }
}

const fn shape(members: &'static [Member]) -> Shape {
    Shape {
        members,
        required: &[],
        variants: &[],
    }
}

/// The root of a client event. Event-specific requirements are in `Session::check`.
pub(crate) static ROOT: Shape = shape(&[
    m("type", "type", S),
    m("event_id", "event_id", S),
    m("previous_item_id", "previous_item_id", S),
    m("item_id", "item_id", S),
    m("response_id", "response_id", S),
    m("content_index", "content_index", I),
    m("audio_end_ms", "audio_end_ms", I),
    m("audio", "audio", S),
    obj("session", "session", O, &SESSION),
    obj("item", "item", O, &ITEM),
    obj("response", "response", O, &RESPONSE),
    // Sideband `session.instructions.append` (voice server controls guide examples).
    m("content", "content", S),
    m("delegation_id", "delegation_id", S | Z),
]);

/// The root members whose subtree is checked only for the event that owns it.
pub(crate) const SUBTREES: [(&str, &str); 3] = [
    ("session", "session.update"),
    ("item", "conversation.item.create"),
    ("response", "response.create"),
];

static SESSION: Shape = Shape {
    members: &[
        m("type", "session.type", S),
        obj("audio", "session.audio", O, &SESSION_AUDIO),
        arr("include", "session.include", A, &STRING),
        m("instructions", "session.instructions", S),
        m("max_output_tokens", "session.max_output_tokens", N | S),
        m("model", "session.model", S),
        arr("output_modalities", "session.output_modalities", A, &STRING),
        m("parallel_tool_calls", "session.parallel_tool_calls", B),
        obj("prompt", "session.prompt", O | Z, &PROMPT),
        obj("reasoning", "session.reasoning", O, &REASONING),
        obj("tool_choice", "session.tool_choice", S | O, &TOOL_CHOICE),
        arr("tools", "session.tools", A, &TOOL),
        obj("tracing", "session.tracing", S | O | Z, &TRACING),
        obj("truncation", "session.truncation", S | O, &TRUNCATION),
    ],
    required: &[],
    variants: &[v("realtime", &[]), v("transcription", &[])],
};

static STRING: Member = m("", "[]", S);

static SESSION_AUDIO: Shape = shape(&[
    obj("input", "audio.input", O, &AUDIO_INPUT),
    obj("output", "audio.output", O, &AUDIO_OUTPUT),
]);

static AUDIO_INPUT: Shape = shape(&[
    obj("format", "audio.input.format", O, &FORMAT),
    obj(
        "noise_reduction",
        "audio.input.noise_reduction",
        O | Z,
        &NOISE_REDUCTION,
    ),
    obj("transcription", "audio.input.transcription", O | Z, &TRANSCRIPTION),
    obj("turn_detection", "audio.input.turn_detection", O | Z, &TURN_DETECTION),
]);

static AUDIO_OUTPUT: Shape = shape(&[
    obj("format", "audio.output.format", O, &FORMAT),
    m("speed", "audio.output.speed", N),
    obj("voice", "audio.output.voice", S | O, &VOICE),
]);

static FORMAT: Shape = Shape {
    members: &[m("type", "format.type", S), m("rate", "format.rate", I)],
    required: &[],
    variants: &[UNTYPED, v("audio/pcm", &[]), v("audio/pcmu", &[]), v("audio/pcma", &[])],
};

static NOISE_REDUCTION: Shape = shape(&[
    m("strength", "noise_reduction.strength", N),
    m("type", "noise_reduction.type", S),
]);

static TRANSCRIPTION: Shape = shape(&[
    m("delay", "transcription.delay", S),
    arr("keywords", "transcription.keywords", A, &STRING),
    m("language", "transcription.language", S),
    arr("languages", "transcription.languages", A, &STRING),
    m("model", "transcription.model", S),
    m("prompt", "transcription.prompt", S),
]);

static TURN_DETECTION: Shape = Shape {
    members: &[
        m("type", "turn_detection.type", S),
        m("create_response", "turn_detection.create_response", B),
        m("idle_timeout_ms", "turn_detection.idle_timeout_ms", I | Z),
        m("interrupt_response", "turn_detection.interrupt_response", B),
        m("prefix_padding_ms", "turn_detection.prefix_padding_ms", I),
        m("silence_duration_ms", "turn_detection.silence_duration_ms", I),
        m("threshold", "turn_detection.threshold", N),
        m("eagerness", "turn_detection.eagerness", S),
    ],
    required: &[],
    variants: &[UNTYPED, v("server_vad", &[]), v("semantic_vad", &[])],
};

static VOICE: Shape = Shape {
    members: &[m("id", "voice.id", S)],
    required: &["id"],
    variants: &[],
};

static PROMPT: Shape = Shape {
    members: &[
        m("id", "prompt.id", S),
        m("version", "prompt.version", S | Z),
        m("variables", "prompt.variables", O | Z),
    ],
    required: &["id"],
    variants: &[],
};

static REASONING: Shape = shape(&[m("effort", "reasoning.effort", S)]);

static TOOL_CHOICE: Shape = Shape {
    members: &[
        m("type", "tool_choice.type", S),
        m("name", "tool_choice.name", S | Z),
        m("server_label", "tool_choice.server_label", S),
    ],
    required: &[],
    variants: &[v("function", &["name"]), v("mcp", &["server_label"])],
};

static TOOL: Member = obj("", "tools[]", O, &TOOL_SHAPE);
static TOOL_SHAPE: Shape = Shape {
    members: &[
        m("type", "tools[].type", S),
        m("name", "tools[].name", S),
        m("description", "tools[].description", S),
        m("parameters", "tools[].parameters", ANY),
        m("server_label", "tools[].server_label", S),
        m("allowed_callers", "tools[].allowed_callers", A | Z),
        m("allowed_tools", "tools[].allowed_tools", A | O | Z),
        m("authorization", "tools[].authorization", S),
        m("connector_id", "tools[].connector_id", S),
        m("defer_loading", "tools[].defer_loading", B),
        m("headers", "tools[].headers", O | Z),
        m("require_approval", "tools[].require_approval", O | S | Z),
        m("server_description", "tools[].server_description", S),
        m("server_url", "tools[].server_url", S),
        m("tunnel_id", "tools[].tunnel_id", S),
    ],
    required: &[],
    variants: &[UNTYPED, v("function", &[]), v("mcp", &["server_label"])],
};

static TRACING: Shape = shape(&[
    m("group_id", "tracing.group_id", S),
    m("metadata", "tracing.metadata", ANY),
    m("workflow_name", "tracing.workflow_name", S),
]);

static TRUNCATION: Shape = Shape {
    members: &[
        m("type", "truncation.type", S),
        m("retention_ratio", "truncation.retention_ratio", N),
        m("token_limits", "truncation.token_limits", O),
    ],
    required: &[],
    variants: &[v("retention_ratio", &["retention_ratio"])],
};

static ITEM: Shape = Shape {
    members: &[
        m("type", "item.type", S),
        m("id", "item.id", S),
        m("object", "item.object", S),
        m("status", "item.status", S),
        m("role", "item.role", S),
        arr("content", "item.content", A, &ITEM_PART),
        m("name", "item.name", S),
        m("arguments", "item.arguments", S),
        m("call_id", "item.call_id", S),
        m("output", "item.output", S | Z),
        m("approval_request_id", "item.approval_request_id", S | Z),
        m("approve", "item.approve", B),
        m("reason", "item.reason", S | Z),
        m("server_label", "item.server_label", S),
        arr("tools", "item.tools", A, &MCP_TOOL),
        m("error", "item.error", O | Z),
    ],
    required: &[],
    variants: &[
        Variant {
            ty: Some("message"),
            roles: &["system", "user", "assistant"],
            required: &["role", "content"],
        },
        v("function_call", &["name", "arguments"]),
        v("function_call_output", &["call_id", "output"]),
        v("mcp_approval_response", &["id", "approval_request_id", "approve"]),
        v("mcp_list_tools", &["server_label", "tools"]),
        v("mcp_call", &["id", "name", "arguments", "server_label"]),
        v("mcp_approval_request", &["id", "arguments", "name", "server_label"]),
    ],
};

/// A conversation item's content part: every member is optional.
static ITEM_PART: Member = obj("", "item.content[]", O, &ITEM_PART_SHAPE);
static ITEM_PART_SHAPE: Shape = shape(&[
    m("type", "item.content[].type", S),
    m("text", "item.content[].text", S),
    m("audio", "item.content[].audio", S),
    m("transcript", "item.content[].transcript", S),
    m("image_url", "item.content[].image_url", S),
    m("detail", "item.content[].detail", S),
]);

static MCP_TOOL: Member = obj("", "item.tools[]", O, &MCP_TOOL_SHAPE);
static MCP_TOOL_SHAPE: Shape = Shape {
    members: &[
        m("name", "item.tools[].name", S),
        m("input_schema", "item.tools[].input_schema", ANY),
        m("description", "item.tools[].description", S | Z),
        m("annotations", "item.tools[].annotations", ANY),
    ],
    required: &["name", "input_schema"],
    variants: &[],
};

static RESPONSE: Shape = shape(&[
    obj("audio", "response.audio", O, &RESPONSE_AUDIO),
    m("conversation", "response.conversation", S),
    arr("input", "response.input", A, &INPUT_ITEM),
    m("instructions", "response.instructions", S),
    m("max_output_tokens", "response.max_output_tokens", N | S),
    m("metadata", "response.metadata", O | Z),
    arr("output_modalities", "response.output_modalities", A, &STRING),
    m("parallel_tool_calls", "response.parallel_tool_calls", B),
    obj("prompt", "response.prompt", O | Z, &PROMPT),
    obj("reasoning", "response.reasoning", O, &REASONING),
    obj("tool_choice", "response.tool_choice", S | O, &TOOL_CHOICE),
    arr("tools", "response.tools", A, &TOOL),
]);

static RESPONSE_AUDIO: Shape = shape(&[obj("output", "audio.output", O, &AUDIO_OUTPUT)]);

static INPUT_ITEM: Member = obj("", "response.input[]", O, &INPUT_ITEM_SHAPE);
static INPUT_ITEM_SHAPE: Shape = Shape {
    members: &[
        m("type", "response.input[].type", S),
        m("role", "response.input[].role", S),
        arr("content", "response.input[].content", A, &INPUT_PART),
        m("id", "response.input[].id", S | Z),
        m("status", "response.input[].status", S | Z),
        m("arguments", "response.input[].arguments", S),
        m("call_id", "response.input[].call_id", S),
        m("name", "response.input[].name", S),
        m("output", "response.input[].output", S),
    ],
    required: &[],
    variants: &[
        Variant {
            ty: Some("message"),
            roles: &["system", "developer", "user", "assistant"],
            required: &["role", "content"],
        },
        v("function_call", &["arguments", "call_id", "name"]),
        v("function_call_output", &["call_id", "output"]),
        v("item_reference", &["id"]),
    ],
};

static INPUT_PART: Member = obj("", "response.input[].content[]", O, &INPUT_PART_SHAPE);
static INPUT_PART_SHAPE: Shape = Shape {
    members: &[
        m("type", "response.input[].content[].type", S),
        m("text", "response.input[].content[].text", S),
        m("audio", "response.input[].content[].audio", S),
        m("transcript", "response.input[].content[].transcript", S | Z),
        m("image_url", "response.input[].content[].image_url", S),
        m("detail", "response.input[].content[].detail", S | Z),
    ],
    required: &[],
    variants: &[
        v("input_text", &["text"]),
        v("input_audio", &["audio"]),
        v("input_image", &["image_url"]),
        v("output_text", &["text"]),
    ],
};
