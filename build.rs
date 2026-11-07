//! Generates the codecs and the schema-type expectations the fixture test uses.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::PathBuf;

use serde_json::Value;

const IND: &str = "    ";

fn main() {
    let opts = hyperschema::Options::new()
        .external("BootRecordV0", "crate::encoding::legacy::BootRecordV0")
        .external("OplogMessageV0", "crate::encoding::legacy::OplogMessageV0")
        .external("OplogMessageV1", "crate::encoding::legacy::OplogMessageV1")
        .external("SystemWriterV0", "crate::encoding::legacy::SystemWriterV0");
    hyperschema::build("spec/autobee/schema.json", "autobee_schema.rs", &opts).unwrap();

    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let fixtures = root.join("fixtures/encoding-reference.json");
    println!("cargo:rerun-if-changed={}", fixtures.display());
    let schema = std::fs::read_to_string(root.join("spec/autobee/schema.json")).unwrap();
    let cases = std::fs::read_to_string(&fixtures).unwrap();
    let out = cases_module(&schema, &cases);
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    std::fs::write(out_dir.join("cases_types.rs"), out).unwrap();
}

// ---------------------------------------------------------------------------
// schema model (only the shapes the autobee schema uses)

struct Model {
    types: HashMap<String, TypeDef>,
}

struct TypeDef {
    module: String,
    name: String,
    kind: Kind,
}

enum Kind {
    Struct(Vec<Field>),
    Array(String),
    Versioned(Vec<(u64, String)>),
    External,
}

struct Field {
    name: String,
    rust: String,
    ty: String,
    required: bool,
    array: bool,
}

fn model(schema: &str) -> Model {
    let json: Value = serde_json::from_str(schema).unwrap();
    let mut types = HashMap::new();
    for t in json["schema"].as_array().unwrap() {
        let name = t["name"].as_str().unwrap();
        let fqn = match t["namespace"].as_str() {
            Some(ns) => format!("@{ns}/{name}"),
            None => name.to_string(),
        };
        let module = t["namespace"]
            .as_str()
            .unwrap_or_default()
            .replace('-', "_");
        let kind = if t["external"].is_string() {
            Kind::External
        } else if let Some(versions) = t["versions"].as_array() {
            Kind::Versioned(
                versions
                    .iter()
                    .map(|v| {
                        (
                            v["version"].as_u64().unwrap(),
                            v["type"].as_str().unwrap().to_string(),
                        )
                    })
                    .collect(),
            )
        } else if t["array"].as_bool() == Some(true) {
            Kind::Array(t["type"].as_str().unwrap().to_string())
        } else {
            let fields = t["fields"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| Field {
                    name: f["name"].as_str().unwrap().to_string(),
                    rust: snake_case(f["name"].as_str().unwrap()),
                    ty: f["type"].as_str().unwrap().to_string(),
                    // bool flag bits are always written, like the reference
                    required: f["required"].as_bool().unwrap_or(false)
                        || (f["array"].as_bool() != Some(true) && f["type"] == "bool"),
                    array: f["array"].as_bool().unwrap_or(false),
                })
                .collect();
            Kind::Struct(fields)
        };
        types.insert(
            fqn,
            TypeDef {
                module,
                name: type_name(name),
                kind,
            },
        );
    }
    Model { types }
}

// ---------------------------------------------------------------------------
// fixtures/encoding-reference.json -> cases_types.rs

fn cases_module(schema: &str, cases: &str) -> String {
    let m = model(schema);
    let json: Value = serde_json::from_str(cases).unwrap();
    let mut body = String::new();

    for case in json["types"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let ty = m
            .types
            .get(name)
            .unwrap_or_else(|| panic!("fixture names unknown type `{name}`"));
        let mut lit = String::new();
        literal(&m, ty, &case["value"], &mut lit);
        let label = format!("{} v{}", name, case["version"].as_u64().unwrap());
        let hex = case["hex"].as_str().unwrap();
        let _ = writeln!(
            body,
            "{IND}{IND}crate::TypeCase {{\n\
             {IND}{IND}{IND}name: {label:?},\n\
             {IND}{IND}{IND}run: Box::new(|| {{\n\
             {IND}{IND}{IND}{IND}let expected = {lit};\n\
             {IND}{IND}{IND}{IND}crate::check(&expected, crate::hex({hex:?}));\n\
             {IND}{IND}{IND}}}),\n\
             {IND}{IND}}},"
        );
    }

    let mut out = String::from("// Generated by build.rs from fixtures/encoding-reference.json.\n");
    let _ = writeln!(out, "pub fn cases() -> Vec<crate::TypeCase> {{");
    let _ = writeln!(out, "{IND}vec![");
    out.push_str(&body);
    let _ = writeln!(out, "{IND}]");
    let _ = writeln!(out, "}}");
    if body.contains("Bytes::") {
        out.insert_str(0, "use bytes::Bytes;\n");
    }
    out
}

fn literal(m: &Model, ty: &TypeDef, json: &Value, out: &mut String) {
    match &ty.kind {
        Kind::Struct(fields) => {
            let _ = write!(
                out,
                "crate::encoding::schema::{}::{} {{ ",
                ty.module, ty.name
            );
            for f in fields {
                let _ = write!(out, "{}: ", f.rust);
                let value = json.get(&f.name).unwrap_or(&Value::Null);
                if f.required {
                    field(m, f, value, out);
                } else if absent(f, value) {
                    out.push_str("None");
                } else {
                    out.push_str("Some(");
                    field(m, f, value, out);
                    out.push(')');
                }
                out.push_str(", ");
            }
            out.push('}');
        }
        Kind::Array(elem) => {
            let _ = write!(
                out,
                "crate::encoding::schema::{}::{}::from(",
                ty.module, ty.name
            );
            array(m, elem, json, out);
            out.push(')');
        }
        Kind::Versioned(versions) => {
            let version = json["version"].as_u64().unwrap_or(0);
            let (wire, inner) = versions
                .iter()
                .find(|(v, _)| *v >= version)
                .unwrap_or_else(|| panic!("{} has no version {version}", ty.name));
            let _ = write!(
                out,
                "crate::encoding::schema::{}::{}::V{wire}(",
                ty.module, ty.name
            );
            let def = m.types.get(inner).unwrap();
            literal(m, def, json, out);
            out.push(')');
        }
        Kind::External => panic!("external type in a fixture: {}", ty.name),
    }
}

/// The reference writes optional fields only when truthy, so an optional number
/// or bool that decodes to zero is absent on the wire.
fn absent(f: &Field, value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Number(n) if n.as_u64() == Some(0) || n.as_i64() == Some(0) => {
            !f.ty.starts_with("fixed") && f.ty != "buffer" && f.ty != "optionalBuffer"
        }
        _ => false,
    }
}

fn field(m: &Model, f: &Field, json: &Value, out: &mut String) {
    if f.array {
        array(m, &f.ty, json, out);
    } else {
        value_literal(m, &f.ty, json, out);
    }
}

fn array(m: &Model, elem: &str, json: &Value, out: &mut String) {
    out.push_str("vec![");
    for item in json.as_array().unwrap() {
        value_literal(m, elem, item, out);
        out.push_str(", ");
    }
    out.push(']');
}

fn value_literal(m: &Model, ty: &str, json: &Value, out: &mut String) {
    match ty {
        "bool" => out.push_str(if json.as_bool() == Some(true) {
            "true"
        } else {
            "false"
        }),
        "uint" | "uint8" | "uint16" | "uint24" | "uint32" | "uint40" | "uint48" | "uint56"
        | "uint64" => {
            let _ = write!(out, "{}", json.as_u64().unwrap_or(0));
        }
        "int" | "int8" | "int16" | "int24" | "int32" | "int40" | "int48" | "int56" | "int64" => {
            let _ = write!(out, "{}", json.as_i64().unwrap_or(0));
        }
        "buffer" => fixed(json, out, |bytes| format!("Bytes::from_static(&{bytes})")),
        "optionalBuffer" => {
            if json.is_null() {
                out.push_str("None");
            } else {
                fixed(json, out, |bytes| {
                    format!("Some(Bytes::from_static(&{bytes}))")
                });
            }
        }
        _ if ty.starts_with("fixed") => {
            let n: usize = ty["fixed".len()..].parse().unwrap();
            let bytes = hex_bytes(json.as_str().unwrap_or_default());
            assert_eq!(bytes.len(), n, "fixed{n} fixture");
            out.push_str(&byte_array(&bytes));
        }
        _ => literal(
            m,
            m.types
                .get(ty)
                .unwrap_or_else(|| panic!("unknown type {ty}")),
            json,
            out,
        ),
    }
}

fn fixed(json: &Value, out: &mut String, f: impl Fn(String) -> String) {
    let bytes = hex_bytes(json.as_str().unwrap_or_default());
    out.push_str(&f(byte_array(&bytes)));
}

fn byte_array(bytes: &[u8]) -> String {
    let mut out = String::from("[");
    for b in bytes {
        let _ = write!(out, "{b}, ");
    }
    out.push(']');
    out
}

fn hex_bytes(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd hex length: {s}");
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).expect("hex digit"))
        .collect()
}

// ---------------------------------------------------------------------------
// naming (mirrors hyperschema-rs so fixtures can name generated fields)

fn type_name(name: &str) -> String {
    let mut out = String::new();
    let mut upper = true;
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            if upper {
                out.extend(c.to_uppercase());
                upper = false;
            } else {
                out.push(c);
            }
        } else {
            upper = true;
        }
    }
    out
}

fn snake_case(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if !c.is_ascii_alphanumeric() {
            if !out.ends_with('_') {
                out.push('_');
            }
        } else if c.is_ascii_uppercase() {
            let boundary = match i.checked_sub(1).map(|j| chars[j]) {
                Some(p) if p.is_ascii_lowercase() || p.is_ascii_digit() => true,
                Some(p) if p.is_ascii_uppercase() => {
                    chars.get(i + 1).is_some_and(|n| n.is_ascii_lowercase())
                }
                _ => false,
            };
            if boundary && !out.is_empty() && !out.ends_with('_') {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    if matches!(out.as_str(), "type" | "match" | "ref" | "self" | "super") {
        out.push('_');
    }
    out
}
