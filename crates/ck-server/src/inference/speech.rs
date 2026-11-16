//! `POST /v1/audio/speech` and `POST /v1/tts`: OpenAI text-to-speech requests served by
//! xAI's unary `/tts` (sdk/api/handlers/openai/openai_speech_handlers.go). The handler
//! rewrites the request into xAI's shape; the raw audio comes back as is.

use std::sync::Arc;

use axum::Extension;
use axum::body::{Body, Bytes};
use axum::extract::rejection::BytesRejection;
use axum::extract::{MatchedPath, OriginalUri, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use ck_core::exec::{Caller, ExecError, FailureScope, Operation};
use ck_core::format::Format;
use ck_wire::gostr::{GoStr, trim_space};
use ck_wire::json::{self as gj, Kind};

use crate::http::gojson::trim;
use crate::inference::claude::read_failed;
use ck_translate::audio;
use ck_translate::codec::document::Document;
use ck_translate::codec::event::LocalId;
use ck_translate::codec::media::Registry;
use ck_translate::codec::{ClientCapability, RequestFence, RequestView};

use crate::http::egress::{self, Framing, Protocol};
use crate::inference::dispatch::{self, Call, Done, Failure, Media, MediaKind};
use crate::inference::images::{bad_request, text};
use crate::inference::streams::{Exchange, Owner};
use crate::{Runtime, inference::errors};

const DEFAULT_MODEL: &str = "grok-tts";
const VOICE_MODEL: &str = "grok-voice-tts-1.0";
const DEFAULT_VOICE: &str = "eve";
const MAX_RUNES: usize = 60_000;
const MAX_BODY: usize = 1 << 20;
const DEFAULT_SAMPLE_RATE: i64 = 24_000;

/// `openAISpeechVoices`: OpenAI voice names on Grok voices. Other names pass through, so
/// Grok voice IDs keep working.
const VOICES: [(&str, &str); 11] = [
    ("alloy", "ara"),
    ("ash", "orion"),
    ("ballad", "luna"),
    ("coral", "celeste"),
    ("echo", "rex"),
    ("fable", "sal"),
    ("onyx", "leo"),
    ("nova", "eve"),
    ("sage", "iris"),
    ("shimmer", "aurora"),
    ("verse", "lumen"),
];

/// `handleXAISpeech`, for both routes. The request is checked and normalized as Go's handler
/// does, decoded by the speech codec into the exchange, and served on the execution contract:
/// the audio streams to the client from its first byte, resumable on `/v1/streams`.
#[allow(clippy::too_many_arguments)]
pub async fn speech(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: Option<MatchedPath>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    limit: crate::http::body_limit::Limit,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let body = match body {
        Ok(body) => body,
        Err(rejection) => return read_failed(&rejection),
    };
    let body = match crate::inference::openai::read_body(&headers, body, limit.0) {
        Ok(body) => body,
        Err(response) => return *response,
    };
    if body.len() > MAX_BODY {
        return bad_request("request body is larger than 1MB");
    }
    let requested = trim(&text(&body, "model")).to_owned();
    let Some(model) = routing_model(&requested) else {
        return bad_request(&format!(
            "Model {requested} is not supported on /v1/audio/speech. Use {DEFAULT_MODEL}."
        ));
    };
    let (request, format) = match normalize(&body, model) {
        Ok(built) => built,
        Err(message) => return bad_request(&message),
    };
    let decoded = match Decoded::new(&rt, &caller, &request) {
        Ok(decoded) => decoded,
        Err(failure) => return errors::openai(&failure),
    };
    let call = Call {
        entry: Format::OpenAI,
        response: Format::OpenAI,
        operation: Operation::Generate,
        model: model.to_owned(),
        body: Bytes::from(request),
        stream: false,
        alt: None,
        headers,
        caller,
        forced_provider: None,
        selection_model: None,
        execution_session: None,
        request_path: dispatch::route_path(matched.as_ref(), &uri),
        peer: dispatch::peer(peer),
        turn: None,
        media: Some(Arc::new(Media {
            kind: MediaKind::Speech,
            pinned: None,
            on_selected: None,
            sse: false,
            disallow_free: false,
        })),
    };
    let Decoded {
        exchange,
        document,
        registry,
        request_id,
    } = decoded;
    let codec = dispatch::Codec {
        exchange,
        view: RequestView {
            document: &document,
            media: &registry,
            operation: OPERATION,
        },
        request_id,
    };
    dispatch::serve_exchange(&rt, call, codec, move |result| async move {
        match result {
            Ok(Done::Buffered { headers, body }) => audio(format, headers.get(header::CONTENT_TYPE).cloned(), body),
            Ok(Done::Stream { .. }) => unreachable!("a non-stream call is buffered"),
            Err(failure) => errors::openai(&failure),
        }
    })
    .await
}

const OPERATION: ck_translate::codec::Operation =
    ck_translate::codec::Operation::Media(ck_translate::codec::Media::Speech);

/// The normalized request decoded into a new exchange's request stream, closed against the
/// client capability (contract G8).
struct Decoded {
    exchange: Exchange,
    document: Document,
    registry: Registry,
    request_id: LocalId,
}

impl Decoded {
    fn new(rt: &Runtime, caller: &Caller, request: &[u8]) -> Result<Self, Failure> {
        let store = rt
            .streams()
            .store()
            .map_err(|error| Failure::Exec(dispatch::stored(&error)))?;
        // The same owner a resume presents (`streams/resume.rs`).
        let owner = Owner {
            caller: caller.clone(),
            scope: None,
        };
        let output = egress::Output {
            protocol: Protocol::Speech,
            operation: OPERATION,
            framing: Framing::Raw,
        };
        let mut exchange =
            Exchange::new(owner, output, &store).map_err(|error| Failure::Exec(dispatch::stored(&error)))?;
        let writer = &mut exchange.request;
        let decoded = (|| {
            let mut decoder = audio::Decoder::new(writer, DECODED_LIMIT)?;
            decoder.advance(writer, request)?;
            let document = decoder.finish(writer)?;
            let registry = Registry::new(writer).map_err(audio::RequestFailure::from)?;
            Ok::<_, audio::RequestFailure>((document, registry))
        })();
        let (document, registry) = decoded.map_err(refused)?;
        let view = RequestView {
            document: &document,
            media: &registry,
            operation: OPERATION,
        };
        RequestFence::default()
            .end(view, &ClientCapability)
            .map_err(|error| refused(error.into()))?;
        let request_id = LocalId::new(&uuid::Uuid::new_v4().simple().to_string()).expect("32 ASCII digits");
        Ok(Self {
            exchange,
            document,
            registry,
            request_id,
        })
    }
}

/// The normalized request is at most six bytes per body byte (an invalid byte becomes
/// `\ufffd`), plus the defaults.
const DECODED_LIMIT: u64 = 8 * MAX_BODY as u64;

/// What the speech codec refused of a normalized request: storage is a resource failure,
/// anything else a gateway defect, since normalization already produced a valid request.
fn refused(failure: audio::RequestFailure) -> Failure {
    let error = match failure.error {
        ck_translate::codec::Error::Storage(error) => dispatch::stored(&error),
        error => {
            tracing::error!(target: "ck_dispatch", %error, "speech request ingress defect");
            ExecError::local(500, FailureScope::Request, "stream.producer_lost")
        }
    };
    Failure::Exec(error)
}

/// The audio response: the upstream Content-Type unless it names no audio type
/// (`speechResponseContentType`), then the format's own.
fn audio(format: &str, upstream: Option<HeaderValue>, body: Bytes) -> Response {
    let content_type = match upstream {
        Some(value) if !content_type_needs_default(value.to_str().unwrap_or_default()) => value,
        _ => HeaderValue::from_static(match format {
            "wav" => "audio/wav",
            "pcm" => "audio/pcm",
            _ => "audio/mpeg",
        }),
    };
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(header::CONTENT_TYPE, content_type);
    response
}

/// `speechContentTypeNeedsDefault`.
fn content_type_needs_default(content_type: &str) -> bool {
    let media = content_type.split(';').next().unwrap_or_default();
    matches!(
        trim(media).go_lower().as_str(),
        "" | "application/json" | "application/octet-stream" | "text/plain"
    )
}

/// `speechRoutingModel`: the OpenAI TTS models and Grok TTS route to `grok-tts`; the
/// voice model routes to itself; anything else is refused.
fn routing_model(model: &str) -> Option<&'static str> {
    match model_base(model).as_str() {
        "" | "tts-1" | "tts-1-hd" | "gpt-4o-mini-tts" | DEFAULT_MODEL => Some(DEFAULT_MODEL),
        VOICE_MODEL => Some(VOICE_MODEL),
        _ => None,
    }
}

/// `speechModelBase`: no provider prefix, no `(...)` suffix, lower case.
fn model_base(model: &str) -> String {
    let mut model = trim(model);
    let lower = model.go_lower();
    if let Some(prefix) = ["xai/", "x-ai/", "grok/"].into_iter().find(|p| lower.starts_with(p)) {
        model = trim(model.get(prefix.len()..).unwrap_or(model));
    }
    if let Some(open) = model.rfind('(')
        && open > 0
        && model.ends_with(')')
    {
        model = trim(&model[..open]);
    }
    trim(model).go_lower()
}

/// `buildXAISpeechPayload`, as the OpenAI speech request the speech codec decodes: Go's
/// checks and defaults (`input` else `text`, at most 60,000 runes; `voice` else `voice_id`,
/// mapped; `language` else `auto`; a positive `speed`; the output format), and the audio format
/// the client asked for. Strings stay Go byte strings: an invalid UTF-8 byte counts as one rune
/// and marshals as one `\ufffd`. MP3 sends no `output_format`.
fn normalize(raw: &[u8], model: &str) -> Result<(Vec<u8>, &'static str), String> {
    if !gj::valid(raw) {
        return Err("body must be valid JSON".into());
    }
    let string = |path: &str| gj::get(raw, path).bytes().into_owned();
    let mut input = trim_space(&string("input")).to_vec();
    if input.is_empty() {
        input = trim_space(&string("text")).to_vec();
    }
    if input.is_empty() {
        return Err("input is required".into());
    }
    let runes: usize = input
        .utf8_chunks()
        .map(|c| c.valid().chars().count() + c.invalid().len())
        .sum();
    if runes > MAX_RUNES {
        return Err("input is longer than 60000 characters".into());
    }
    let mut voice = trim_space(&string("voice")).to_vec();
    if voice.is_empty() {
        voice = string("voice_id");
    }
    let mut language = trim_space(&string("language")).to_vec();
    if language.is_empty() {
        language = b"auto".to_vec();
    }
    let (format, output) = output_format(raw)?;
    let speed = gj::get(raw, "speed");
    let speed = if speed.kind == Kind::Number && speed.float() > 0.0 {
        let value = speed.float();
        // json.Marshal refuses an infinite float64 (a number too large to parse).
        Some(gj::json_float(value).ok_or_else(|| format!("json: unsupported value: {}", gj::fmt_float(value)))?)
    } else {
        None
    };
    let mut body = br#"{"model":"#.to_vec();
    body.extend(gj::quote(model.as_bytes()));
    body.extend(br#","input":"#);
    body.extend(gj::quote(&input));
    body.extend(br#","voice":"#);
    body.extend(gj::quote(map_voice(&voice).as_bytes()));
    body.extend(br#","language":"#);
    body.extend(gj::quote(&language));
    if let Some(speed) = &speed {
        body.extend(format!(r#","speed":{speed}"#).bytes());
    }
    if let Some((codec, rate)) = output {
        body.extend(format!(r#","output_format":{{"codec":"{codec}","sample_rate":{rate}}}"#).bytes());
    }
    body.push(b'}');
    Ok((body, format))
}

/// `mapXAISpeechVoice`: lower case (Go's `strings.ToLower` turns each invalid byte into
/// U+FFFD), OpenAI names mapped.
fn map_voice(voice: &[u8]) -> String {
    let voice: String = trim_space(voice)
        .utf8_chunks()
        .flat_map(|c| {
            c.valid()
                .chars()
                .chain(std::iter::repeat_n('\u{FFFD}', c.invalid().len()))
        })
        .collect::<String>()
        .go_lower();
    if voice.is_empty() {
        return DEFAULT_VOICE.into();
    }
    VOICES
        .iter()
        .find(|(openai, _)| *openai == voice)
        .map_or(voice.clone(), |(_, grok)| (*grok).to_owned())
}

/// `speechOutputFormat`: `response_format` (at 24 kHz), else a native `output_format`
/// object, else MP3. MP3 sends no `output_format`.
type Output = Option<(&'static str, i64)>;

fn output_format(raw: &[u8]) -> Result<(&'static str, Output), String> {
    let response_format = trim(&text(raw, "response_format")).go_lower();
    if !response_format.is_empty() {
        return codec_format(&response_format, DEFAULT_SAMPLE_RATE);
    }
    let native = gj::get(raw, "output_format");
    if !native.exists() {
        return Ok(("mp3", None));
    }
    if !native.is_object() {
        return Err("output_format must be an object".into());
    }
    let codec = trim(&native.get("codec").str()).go_lower();
    let mut rate = DEFAULT_SAMPLE_RATE;
    let given = native.get("sample_rate");
    if given.exists() && given.kind == Kind::Number && given.float() > 0.0 {
        // Go's `int(float64)`: truncation, and MinInt64 past the range on amd64.
        let value = given.float();
        rate = if value < i64::MAX as f64 {
            value as i64
        } else {
            i64::MIN
        };
    }
    codec_format(&codec, rate)
}

/// `speechCodecFormat`.
fn codec_format(codec: &str, rate: i64) -> Result<(&'static str, Output), String> {
    let rate = if rate <= 0 { DEFAULT_SAMPLE_RATE } else { rate };
    match codec {
        "" | "mp3" => Ok(("mp3", None)),
        "wav" => Ok(("wav", Some(("wav", rate)))),
        "pcm" => Ok(("pcm", Some(("pcm", rate)))),
        _ => Err("response_format must be mp3, wav, or pcm".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Under passthrough the upstream type wins unless it names no audio; the speech
    /// goldens run without passthrough, so only the format default reaches them.
    #[test]
    fn upstream_content_type_kept_only_when_it_names_audio() {
        let content_type = |format, upstream: Option<&'static str>| {
            let response = audio(format, upstream.map(HeaderValue::from_static), Bytes::new());
            response.headers()[header::CONTENT_TYPE].to_str().unwrap().to_owned()
        };
        assert_eq!(
            content_type("wav", Some("audio/ogg; codecs=opus")),
            "audio/ogg; codecs=opus"
        );
        assert_eq!(
            content_type("wav", Some(" Application/JSON ; charset=utf-8")),
            "audio/wav"
        );
        assert_eq!(content_type("pcm", Some("text/plain")), "audio/pcm");
        assert_eq!(content_type("mp3", Some("application/octet-stream")), "audio/mpeg");
        assert_eq!(content_type("pcm", None), "audio/pcm");
    }
}
