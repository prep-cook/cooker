import contextlib
import importlib.util
import io
import pathlib
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "check_rust_size", pathlib.Path(__file__).with_name("check-rust-size.py"))
size = importlib.util.module_from_spec(spec)
spec.loader.exec_module(size)


def body(lines):
    """A function `name` spanning exactly `lines` lines."""
    return "fn name() {\n" + "    let _ = 1;\n" * (lines - 2) + "}\n"


def spans(text):
    return {name: end - start + 1 for name, start, end in size.functions(text)}


class Parser(unittest.TestCase):
    def test_braces_inside_literals_and_comments_are_ignored(self):
        text = '''fn tricky<'a>(x: &'a str) -> char {
    let a = "}\\"{";
    let b = r#"}"{"#;
    let c = r##"}"# {"##;
    let d = b"}";
    let e = br#"{"#;
    let f = c"}";
    let g = '{';
    let h = '}';
    let i = '\\'';
    let j = b'{';
    let k = '\\u{7D}';
    // }
    /* } /* nested } */ { */
    let r#type = 1;
    'outer: loop { break 'outer; }
    '}'
}
fn after() {}
'''
        self.assertEqual(spans(text), {"tricky": 18, "after": 1})

    def test_each_literal_is_one_token(self):
        for text in ('r#"a"{"#', 'r"\\"', 'br##"a"#}"##', '"a\\"}"', "'\\''", "'\\\\'", "'\\u{7D}'",
                     "b'\\''", "'{'", "1_000u64"):
            with self.subTest(text=text):
                self.assertEqual(size.tokens(text), [("lit", "", 1)])

    def test_lifetimes_raw_identifiers_and_lines(self):
        self.assertEqual(size.tokens("'a: r#fn\n/* \n */ \"x\ny\" 'b"), [
            ("lifetime", "'a", 1), ("punct", ":", 1), ("ident", "r#fn", 1),
            ("lit", "", 3), ("lifetime", "'b", 4),
        ])

    def test_names_are_qualified_and_repeats_numbered(self):
        text = '''mod outer {
    pub struct S;
    impl S {
        pub fn new() -> Self { S }
    }
    impl<T: Into<u8>> std::fmt::Display for Wrapper<T> where T: Copy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { Ok(()) }
    }
    impl S {
        pub fn new() -> Self { S }
    }
    pub trait Tr {
        fn declared(&self);
        fn provided(&self) -> fn(u8) -> u8 { |x| x }
    }
    #[cfg(test)]
    mod tests {
        #[test]
        fn outer_fn() {
            fn inner() {}
            let closure = || { 1 };
        }
    }
}
extern "C" {
    fn ffi(x: u8);
}
'''
        self.assertEqual(sorted(size.functions(text)), [
            ("outer::<Wrapper as Display>::fmt", 7, 7),
            ("outer::S::new", 4, 4),
            ("outer::S::new", 10, 10),
            ("outer::Tr::provided", 14, 14),
            ("outer::tests::outer_fn", 19, 22),
            ("outer::tests::outer_fn::inner", 20, 20),
        ])
        with tempfile.TemporaryDirectory() as root:
            pathlib.Path(root, "a.rs").write_text(text)
            names = {key for kind, key in size.measure(root) if kind == "fn"}
        self.assertIn("a.rs::outer::S::new", names)
        self.assertIn("a.rs::outer::S::new#2", names)

    def test_signature_brackets_do_not_open_the_body(self):
        text = "fn f<const N: usize>(a: [u8; N]) -> [u8; 4]\nwhere\n    [u8; N]: Sized,\n{\n    [0; 4]\n}\n"
        self.assertEqual(spans(text), {"f": 6})

    def test_comparisons_and_shifts_in_const_expressions_are_not_generics(self):
        text = '''struct S<const N: usize = { 1 >> 0 }, const M: bool = { 1 < 2 }>;
fn short(_: [u8; { if 1 < 2 { 1 } else { 0 } }]) {}
fn shift(_: [u8; 1 >> 0], _: [Vec<u8>; 2 << 1]) -> Option<Vec<Vec<u8>>>
{
    None
}
fn arg(_: Foo<{ 1 < 2 }>, _: Foo<{ 4 >> 1 }>) {}
impl<const N: usize> Tr for Foo<{ N >> 1 }> where [(); N < 3]: Sized {
    fn x() {}
}
'''
        self.assertEqual(spans(text), {"short": 1, "shift": 4, "arg": 1, "<Foo as Tr>::x": 1})

    def test_macro_generated_functions_are_measured(self):
        text = '''macro_rules! make {
    ($name:ident) => {
        fn $name() {
            let _ = 1;
        }
    };
    (fn $name:ident $body:block) => { fn $name() $body };
    ($name:ident, $t:ty) => { fn $name() -> $t { <$t>::default() } };
}
make!(long);
fn after() {}
'''
        self.assertEqual(sorted(size.functions(text)), [
            ("after", 11, 11), ("make!::$name", 3, 5), ("make!::$name", 8, 8)])
        self.write_tree("macro_rules! make { ($name:ident) => { " + body(81).replace("fn name", "fn $name")
                        + "}; }\nmake!(long);\n")
        code, err = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("fn a.rs::make!::$name: 81 lines, over the 80-line cap", err)

    def write_tree(self, text):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = pathlib.Path(tmp.name)
        (self.root / "tools").mkdir()
        (self.root / size.RATCHET).write_text("")
        (self.root / "a.rs").write_text(text)

    def gate(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = size.main(["--root", str(self.root)])
        return code, err.getvalue()

    def test_unclosed_input_cannot_run(self):
        for text in ("fn f() {\n", "fn f() {}\n}\n", 'fn f() { "open }\n', "fn f() { /* open }\n",
                     'fn f() { r#"open }\n'):
            with self.subTest(text=text), self.assertRaises(size.CannotRun):
                size.functions(text)


class Gate(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="cooker-rust-size-")
        self.addCleanup(self._tmp.cleanup)
        self.root = pathlib.Path(self._tmp.name)
        (self.root / "crates/a/src").mkdir(parents=True)
        (self.root / "tools").mkdir()
        self.write("crates/a/src/lib.rs", body(10))
        self.ratchet("")

    def write(self, rel, text):
        (self.root / rel).write_text(text)

    def ratchet(self, text):
        (self.root / size.RATCHET).write_text(text)

    def run_gate(self, *args):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = size.main(["--root", str(self.root), *args])
        return code, err.getvalue()

    def test_clean_tree_passes(self):
        self.assertEqual(self.run_gate(), (0, ""))

    def test_new_function_over_the_cap_fails(self):
        self.write("crates/a/src/lib.rs", body(81))
        code, err = self.run_gate()
        self.assertEqual(code, 1)
        self.assertIn("fn crates/a/src/lib.rs::name: 81 lines, over the 80-line cap", err)
        self.write("crates/a/src/lib.rs", body(80))
        self.assertEqual(self.run_gate()[0], 0)

    def test_new_file_over_the_cap_fails(self):
        self.write("crates/a/src/big.rs", "// line\n" * 601)
        code, err = self.run_gate()
        self.assertEqual(code, 1)
        self.assertIn("file crates/a/src/big.rs: 601 lines, over the 600-line cap", err)

    def test_tests_vendor_and_generated_files(self):
        (self.root / "crates/a/tests").mkdir()
        self.write("crates/a/tests/it.rs", body(81))
        self.assertEqual(self.run_gate()[0], 1)
        (self.root / "crates/a/tests/it.rs").unlink()
        for rel in ("vendor/x/src", "target/debug", ".cache"):
            (self.root / rel).mkdir(parents=True)
            self.write(f"{rel}/big.rs", body(81))
        self.write("crates/a/src/tables.rs", "// @generated by gen.sh. Do not edit.\n" + body(81))
        self.assertEqual(self.run_gate(), (0, ""))

    def test_ratcheted_item_may_not_grow(self):
        self.write("crates/a/src/lib.rs", body(90))
        self.ratchet("fn crates/a/src/lib.rs::name 89\n")
        code, err = self.run_gate()
        self.assertEqual(code, 1)
        self.assertIn("90 lines, grew past its ratchet entry of 89", err)

    def test_stale_high_entry_fails_with_the_replacement_line(self):
        self.write("crates/a/src/lib.rs", body(85))
        self.ratchet("fn crates/a/src/lib.rs::name 90\n")
        code, err = self.run_gate()
        self.assertEqual(code, 1)
        self.assertIn("lower the entry to `fn crates/a/src/lib.rs::name 85`", err)
        self.ratchet("fn crates/a/src/lib.rs::name 85\n")
        self.assertEqual(self.run_gate(), (0, ""))

    def test_entry_within_the_cap_fails(self):
        self.ratchet("fn crates/a/src/lib.rs::name 90\n")
        code, err = self.run_gate()
        self.assertEqual(code, 1)
        self.assertIn("within the 80-line cap; remove its ratchet entry", err)

    def test_renamed_or_missing_entry_fails(self):
        self.write("crates/a/src/lib.rs", body(90).replace("fn name", "fn renamed"))
        self.ratchet("fn crates/a/src/lib.rs::name 90\nfile crates/a/src/gone.rs 700\n")
        code, err = self.run_gate()
        self.assertEqual(code, 1)
        self.assertIn("fn crates/a/src/lib.rs::name: in the ratchet but not in the tree", err)
        self.assertIn("file crates/a/src/gone.rs: in the ratchet but not in the tree", err)
        self.assertIn("fn crates/a/src/lib.rs::renamed: 90 lines, over the 80-line cap", err)

    def test_update_lowers_and_removes_but_never_adds_or_raises(self):
        self.write("crates/a/src/lib.rs", body(85))
        self.write("crates/a/src/grown.rs", body(95))
        self.write("crates/a/src/new.rs", body(99))
        self.ratchet(
            "# header\n"
            "fn crates/a/src/lib.rs::name 90\n"
            "fn crates/a/src/grown.rs::name 91\n"
            "file crates/a/src/gone.rs 700\n")
        code, err = self.run_gate("--update")
        self.assertEqual((self.root / size.RATCHET).read_text(),
                         "# header\nfn crates/a/src/lib.rs::name 85\nfn crates/a/src/grown.rs::name 91\n")
        self.assertEqual(code, 1)
        self.assertIn("fn crates/a/src/grown.rs::name: 95 lines, grew past its ratchet entry of 91", err)
        self.assertIn("fn crates/a/src/new.rs::name: 99 lines, over the 80-line cap", err)

    def test_cannot_run(self):
        (self.root / "crates/a/src/lib.rs").unlink()
        self.assertEqual(self.run_gate()[0], 2, "no Rust file")
        self.write("crates/a/src/lib.rs", "fn f() {\n")
        self.assertEqual(self.run_gate()[0], 2, "unclosed brace")
        self.write("crates/a/src/lib.rs", body(10))
        for text in ("fn crates/a/src/lib.rs::name\n", "fun x 90\n", "fn x 90\nfn x 91\n"):
            self.ratchet(text)
            self.assertEqual(self.run_gate()[0], 2, text)
        (self.root / size.RATCHET).unlink()
        self.assertEqual(self.run_gate()[0], 2, "no ratchet file")


if __name__ == "__main__":
    unittest.main()
