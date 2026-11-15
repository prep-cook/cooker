//! In-place edits of a Kimi upload. Kimi's shaping replaces or removes spans of the one body
//! it sends, instead of rebuilding the body from collected copies of its parts.

use std::cmp::Reverse;
use std::ops::Range;

use ck_wire::json::Res;

/// A span of the body and the bytes that replace it.
pub(crate) type Edit = (Range<usize>, Vec<u8>);

/// The bytes of the body that `value`, read from that body, occupies.
pub(crate) fn span(value: &Res<'_>) -> Range<usize> {
    value.index..value.index + value.raw.len()
}

/// Replaces each span with its bytes. The spans are disjoint; the last is replaced first, so
/// the offsets of the others stay valid.
pub(crate) fn splice(body: &mut Vec<u8>, mut edits: Vec<Edit>) {
    edits.sort_unstable_by_key(|(range, _)| Reverse(range.start));
    for (range, bytes) in edits {
        body.splice(range, bytes);
    }
}

/// The spans that remove the `dropped` elements of an array whose elements occupy `elements`,
/// with the separators that would be left over. A run of dropped elements goes with the
/// separator after it, or, at the end of the array, with the one before it.
pub(crate) fn removals(elements: &[Range<usize>], dropped: &[bool]) -> Vec<Range<usize>> {
    let mut spans = Vec::new();
    let mut i = 0;
    while i < elements.len() {
        if !dropped[i] {
            i += 1;
            continue;
        }
        let first = i;
        while i < elements.len() && dropped[i] {
            i += 1;
        }
        let last = &elements[i - 1];
        spans.push(match (elements.get(i), first.checked_sub(1)) {
            (Some(next), _) => elements[first].start..next.start,
            (None, Some(kept)) => elements[kept].end..last.end,
            (None, None) => elements[first].start..last.end,
        });
    }
    spans
}

/// Removes the root member whose value occupies `value` (its name has no escapes), with the
/// comma that separates it from a neighbour.
pub(crate) fn remove_member(body: &mut Vec<u8>, value: Range<usize>) {
    let blank = |b: &u8| b.is_ascii_whitespace();
    let before = |end: usize| body[..end].iter().rposition(|b| !blank(b));
    // `"name"` `:` value: the colon, the closing quote, then the opening quote.
    let Some(colon) = before(value.start) else { return };
    let Some(close) = before(colon) else { return };
    let Some(open) = body[..close].iter().rposition(|&b| b == b'"') else {
        return;
    };
    let after = body[value.end..].iter().position(|b| !blank(b)).map(|i| value.end + i);
    let range = match after {
        Some(comma) if body[comma] == b',' => open..comma + 1,
        _ => match before(open) {
            Some(comma) if body[comma] == b',' => comma..value.end,
            _ => open..value.end,
        },
    };
    body.drain(range);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spans(json: &str) -> Vec<Range<usize>> {
        ck_wire::json::get(json.as_bytes(), "a")
            .array()
            .iter()
            .map(span)
            .collect()
    }

    fn dropping(json: &str, dropped: &[bool]) -> String {
        let mut body = json.as_bytes().to_vec();
        let edits = removals(&spans(json), dropped)
            .into_iter()
            .map(|r| (r, Vec::new()))
            .collect();
        splice(&mut body, edits);
        String::from_utf8(body).unwrap()
    }

    #[test]
    fn removals_leave_a_valid_array() {
        let json = r#"{"a":[1, 2, 3, 4]}"#;
        for (dropped, want) in [
            ([true, false, false, false], r#"{"a":[2, 3, 4]}"#),
            ([true, true, false, false], r#"{"a":[3, 4]}"#),
            ([false, true, true, false], r#"{"a":[1, 4]}"#),
            ([false, false, true, true], r#"{"a":[1, 2]}"#),
            ([true, false, true, true], r#"{"a":[2]}"#),
            ([true, true, true, true], r#"{"a":[]}"#),
            ([false, false, false, false], json),
        ] {
            assert_eq!(dropping(json, &dropped), want, "{dropped:?}");
        }
    }

    #[test]
    fn a_member_goes_with_one_comma() {
        for (json, want) in [
            (r#"{"stream":false,"a":1}"#, r#"{"a":1}"#),
            (r#"{"a":1, "stream" : false}"#, r#"{"a":1}"#),
            (r#"{"a":1,"stream":true,"b":2}"#, r#"{"a":1,"b":2}"#),
            (r#"{"stream":false}"#, r#"{}"#),
        ] {
            let mut body = json.as_bytes().to_vec();
            let value = span(&ck_wire::json::get(&body, "stream"));
            remove_member(&mut body, value);
            assert_eq!(String::from_utf8(body).unwrap(), want);
        }
    }
}
