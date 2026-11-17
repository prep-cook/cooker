//! sjson edits made inside the document's own buffer. sjson rebuilds the whole document for
//! every edit; for the edits whose result is the document with one range replaced, [`edit`]
//! replaces that range instead and produces the same bytes. Those edits are a set or delete
//! of a value the path reaches, and a set that adds a member to the deepest object the path
//! reaches. Everything else (arrays on the way, `-1`, complex paths, a missing or non-object
//! container, whitespace around the document) goes back to `set_impl`.

use std::ops::Range;

use super::{Kind, PathPart, append_build, append_stringify, atoui, delete_tail_item, get, parse, split_path};

/// Room reserved beyond an edit that outgrows the buffer, so the next few small insertions
/// into the same document fit without another reallocation.
const SLACK: usize = 256;

/// Applies the edit as `set_impl` would, in place. False when it is not one of the edits
/// above and the caller has to rebuild the document.
pub(super) fn edit(out: &mut Vec<u8>, path: &[u8], raw: &[u8], stringify: bool, del: bool) -> bool {
    if path.is_empty() {
        return false;
    }
    let Some(paths) = split_path(path) else {
        return false;
    };
    // The container the current part is looked up in: the document, then each value found.
    let mut container = 0..out.len();
    for (i, part) in paths.iter().enumerate() {
        if del && part.part == b"-1" && !part.force {
            return false;
        }
        let res = get(&out[container.clone()], &part.gpart);
        if res.index == 0 {
            // A missing delete target is sjson's no-op.
            return del || insert(out, container, &paths[i..], raw, stringify);
        }
        let value = container.start + res.index..container.start + res.index + res.raw.len();
        if i + 1 < paths.len() {
            container = value;
            continue;
        }
        if del {
            remove(out, value);
        } else {
            let mut bytes = Vec::with_capacity(raw.len() + 2);
            if stringify {
                append_stringify(&mut bytes, raw);
            } else {
                bytes.extend_from_slice(raw);
            }
            replace(out, value, &bytes);
        }
        return true;
    }
    false
}

/// sjson's new member at the end of `container`, which must be exactly one JSON object:
/// `set_impl` drops whitespace around the document and rebuilds anything else.
fn insert(out: &mut Vec<u8>, container: Range<usize>, paths: &[PathPart], raw: &[u8], stringify: bool) -> bool {
    let json = &out[container.clone()];
    let parsed = parse(json);
    let exact = parsed.kind == Kind::Json && parsed.raw().as_ptr() == json.as_ptr() && parsed.raw().len() == json.len();
    if atoui(&paths[0]).1 || !exact || json.first() != Some(&b'{') || json.last() != Some(&b'}') {
        return false;
    }
    let comma = json[1..]
        .iter()
        .find(|&&c| c > b' ')
        .is_some_and(|&c| c != b'}' && c != b']');
    let mut member = Vec::new();
    if comma {
        member.push(b',');
    }
    append_build(&mut member, false, paths, raw, stringify);
    let close = container.end - 1;
    replace(out, close..close, &member);
    true
}

/// sjson's delete of the value at `value`, with its key and one comma.
fn remove(out: &mut Vec<u8>, value: Range<usize>) {
    let (keep, item) = delete_tail_item(&out[..value.start]);
    let mut end = value.end;
    if item
        && let Some(next) = out[end..].iter().position(|&c| c > b' ')
        && out[end + next] == b','
    {
        end += next + 1;
    }
    out.drain(keep..end);
}

/// Writes `bytes` over `range`; an equal value leaves the buffer untouched.
fn replace(out: &mut Vec<u8>, range: Range<usize>, bytes: &[u8]) {
    if out[range.clone()] == *bytes {
        return;
    }
    let grow = bytes.len().saturating_sub(range.len());
    if out.capacity() - out.len() < grow {
        out.reserve_exact(grow + SLACK);
    }
    out.splice(range, bytes.iter().copied());
}

#[cfg(test)]
mod tests {
    use super::super::{set_impl, try_delete};
    use super::*;

    /// The in-place edit against sjson's rebuild, for each edit kind it takes and some it
    /// declines.
    #[test]
    fn edits_match_the_rebuild() {
        let docs: &[&[u8]] = &[
            br#"{"a":1,"b":{"c":"x","d":[1,2]},"e":{}}"#,
            br#"{ "a" : 1 , "b" : { "c" : "x" } }"#,
            br#" {"a":1} "#,
            br#"{"a":{"b":1},"a":{"c":2}}"#,
            br#"{"b":"s"}"#,
            br#"[1,{"a":2}]"#,
            b"{}",
            b"",
        ];
        let paths: &[&str] = &[
            "a", "b.c", "b.d", "b.d.1", "b.d.-1", "e.f", "e.f.g", "z", "b.z.y", "a.c", "b.c.z", "x\\.y", ":1",
        ];
        for doc in docs {
            for path in paths {
                for (raw, stringify, del) in [
                    (&b"\"x\""[..], false, false),
                    (b"x", true, false),
                    (b"7", false, false),
                    (b"", false, true),
                ] {
                    let want = set_impl(doc, path.as_bytes(), raw, stringify, del).unwrap_or_else(|_| doc.to_vec());
                    let mut got = doc.to_vec();
                    if edit(&mut got, path.as_bytes(), raw, stringify, del) {
                        assert_eq!(
                            String::from_utf8_lossy(&got),
                            String::from_utf8_lossy(&want),
                            "{} {path} {} del={del}",
                            String::from_utf8_lossy(doc),
                            String::from_utf8_lossy(raw)
                        );
                    }
                }
            }
        }
        assert_eq!(try_delete(br#"{"a":1}"#, "a").unwrap(), b"{}");
    }

    /// An equal value or a missing delete target changes nothing; a member added to a
    /// buffer without room reallocates once, with room for the next.
    #[test]
    fn edits_stay_in_the_buffer() {
        let mut doc = br#"{"r":{"effort":"high"},"m":"x"}"#.to_vec();
        let ptr = doc.as_ptr();
        assert!(edit(&mut doc, b"r.effort", b"high", true, false));
        assert!(edit(&mut doc, b"r.missing", b"", false, true));
        assert_eq!(doc.as_ptr(), ptr);
        assert!(edit(&mut doc, b"r.summary", b"auto", true, false));
        let capacity = doc.capacity();
        assert!(edit(&mut doc, b"r.extra", b"1", false, false));
        assert_eq!(doc.capacity(), capacity);
        assert_eq!(doc, br#"{"r":{"effort":"high","summary":"auto","extra":1},"m":"x"}"#);
    }
}
