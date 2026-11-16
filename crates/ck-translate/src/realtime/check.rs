//! Per-container state of the client schema check ([`super::schema`]), driven by the scan.
//!
//! A fault inside `session`, `item` or `response` is kept for that subtree and refuses the
//! message only when the event owns the subtree ([`SUBTREES`]), because the event `type` may
//! follow the subtree in the message. A fault elsewhere refuses the message at once.

use super::{
    envelope::Text,
    scan::{Fault, MAX_DEPTH},
    schema::{I, Member, N, O, ROOT, SUBTREES, Shape},
};

static ROOT_MEMBER: Member = Member {
    name: "",
    param: "",
    kinds: O,
    object: Some(&ROOT),
    items: None,
};

#[derive(Clone, Copy)]
struct Node {
    shape: Option<&'static Shape>,
    items: Option<&'static Member>,
    /// Members of `shape` seen so far, by index.
    present: u64,
    ty: Text,
    role: Text,
    subtree: Option<usize>,
}

const EMPTY: Node = Node {
    shape: None,
    items: None,
    present: 0,
    ty: Text::Absent,
    role: Text::Absent,
    subtree: None,
};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Capture {
    None,
    Type,
    Role,
}

pub(super) struct Check {
    nodes: [Node; MAX_DEPTH],
    /// The documented member of the value being read, if any.
    spec: Option<&'static Member>,
    capture: Capture,
    pub(super) faults: [Option<Fault>; SUBTREES.len()],
}

impl Check {
    /// A check of a client message when `validate`, else one that checks nothing.
    pub(super) fn new(validate: bool) -> Self {
        Self {
            nodes: [EMPTY; MAX_DEPTH],
            spec: validate.then_some(&ROOT_MEMBER),
            capture: Capture::None,
            faults: [None; SUBTREES.len()],
        }
    }

    pub(super) fn capturing(&self) -> bool {
        self.capture != Capture::None
    }

    /// Refuses at once outside a subtree; inside one, keeps the subtree's first fault.
    fn fault(&mut self, depth: usize, fault: Fault) -> Result<(), Fault> {
        match depth.checked_sub(1).and_then(|top| self.nodes[top].subtree) {
            Some(subtree) => {
                self.faults[subtree].get_or_insert(fault);
                Ok(())
            }
            None => Err(fault),
        }
    }

    /// A member key of the object at `depth`.
    pub(super) fn key(&mut self, depth: usize, key: &[u8]) -> Result<(), Fault> {
        let node = &mut self.nodes[depth - 1];
        self.spec = None;
        self.capture = Capture::None;
        let Some((index, member)) = node.shape.and_then(|shape| shape.member(key)) else {
            return Ok(());
        };
        let repeated = node.present & (1 << index) != 0;
        node.present |= 1 << index;
        self.spec = Some(member);
        if node.shape.is_some_and(|shape| !shape.variants.is_empty()) {
            self.capture = match key {
                b"type" => Capture::Type,
                b"role" => Capture::Role,
                _ => Capture::None,
            };
        }
        if repeated {
            return self.fault(depth, Fault::Duplicate(member.param));
        }
        Ok(())
    }

    /// The next element of the array at `depth`.
    pub(super) fn element(&mut self, depth: usize) {
        self.spec = self.nodes[depth - 1].items;
        self.capture = Capture::None;
    }

    /// A value of JSON kind `kind` starts in the container at `depth`.
    pub(super) fn value(&mut self, depth: usize, kind: u8) -> Result<(), Fault> {
        if kind != super::schema::S {
            self.capture = Capture::None;
        }
        match self.spec {
            Some(spec) if spec.kinds & kind == 0 => self.fault(depth, Fault::Kind(spec.param)),
            _ => Ok(()),
        }
    }

    /// A string value starts; a discriminator is captured from it.
    pub(super) fn string(&mut self, depth: usize) {
        if let Some(text) = self.captured(depth) {
            *text = Text::Value([0; Text::MAX], 0);
        }
    }

    pub(super) fn char(&mut self, depth: usize, c: char) {
        if let Some(text) = self.captured(depth) {
            text.push(c);
        }
    }

    pub(super) fn end_string(&mut self) {
        self.capture = Capture::None;
    }

    fn captured(&mut self, depth: usize) -> Option<&mut Text> {
        let node = &mut self.nodes[depth.checked_sub(1)?];
        match self.capture {
            Capture::Type => Some(&mut node.ty),
            Capture::Role => Some(&mut node.role),
            Capture::None => None,
        }
    }

    /// A number ends; `integer` when it has no fraction or exponent.
    pub(super) fn number(&mut self, depth: usize, integer: bool) -> Result<(), Fault> {
        match self.spec {
            Some(spec) if !integer && spec.kinds & N == 0 && spec.kinds & I != 0 => {
                self.fault(depth, Fault::Kind(spec.param))
            }
            _ => Ok(()),
        }
    }

    /// A container opens at index `depth` (the root at 0).
    pub(super) fn open(&mut self, depth: usize, array: bool) {
        let spec = self.spec.take();
        let parent = depth.checked_sub(1).map(|top| self.nodes[top]);
        let subtree = match (parent, spec) {
            (Some(parent), Some(spec)) if parent.shape.is_some_and(|s| std::ptr::eq(s, &ROOT)) => {
                SUBTREES.iter().position(|(name, _)| *name == spec.name)
            }
            (Some(parent), _) => parent.subtree,
            (None, _) => None,
        };
        self.nodes[depth] = Node {
            shape: spec.filter(|_| !array).and_then(|spec| spec.object),
            items: spec.filter(|_| array).and_then(|spec| spec.items),
            subtree,
            ..EMPTY
        };
        if array {
            self.spec = self.nodes[depth].items;
        }
    }

    /// The object at `depth` closes: its required members and variant are checked.
    pub(super) fn close(&mut self, depth: usize) -> Result<(), Fault> {
        let node = self.nodes[depth - 1];
        let Some(shape) = node.shape else {
            return Ok(());
        };
        match variant(shape, &node) {
            Ok(()) => Ok(()),
            Err(fault) => self.fault(depth, fault),
        }
    }
}

fn variant(shape: &'static Shape, node: &Node) -> Result<(), Fault> {
    let present = |name: &str| {
        shape
            .member(name.as_bytes())
            .is_some_and(|(index, _)| node.present & (1 << index) != 0)
    };
    let missing = |names: &[&str]| match names.iter().find(|name| !present(name)) {
        Some(name) => Err(Fault::Missing(shape.param(name))),
        None => Ok(()),
    };
    missing(shape.required)?;
    if shape.variants.is_empty() {
        return Ok(());
    }
    let ty = node.ty.get();
    let Some(variant) = shape.variants.iter().find(|v| match (&node.ty, v.ty) {
        (Text::Absent, None) => true,
        (_, Some(name)) => ty == Some(name),
        _ => false,
    }) else {
        return Err(match node.ty {
            Text::Absent => Fault::Missing(shape.param("type")),
            _ => Fault::Value(shape.param("type")),
        });
    };
    missing(variant.required)?;
    if !variant.roles.is_empty() && !node.role.get().is_some_and(|role| variant.roles.contains(&role)) {
        return Err(Fault::Value(shape.param("role")));
    }
    Ok(())
}
