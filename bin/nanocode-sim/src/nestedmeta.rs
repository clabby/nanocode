use std::{collections::BTreeSet, ops::Range, process::ExitCode, sync::Arc};

use nanocode_backend_codex::{
    Model,
    client::{ModelConfig, sim as prompt_sim},
};
use nanocode_code_mode::{ToolContext, ToolOutput};
use nanocode_sync::{Slot, block_on};
use nanocode_tool_runtime::{
    CodeModeToolPolicy, ESCALATION_INPUTS, FileAccess, Tools,
    runtime::ToolRuntime,
    sim::{self as host_sim, Mutation as Rule},
};
use serde_json::{Value, json};

use crate::{
    executor::Clock,
    rng::{FNV_OFFSET, Rng, fnv},
};

const SESSION: &str = "nested-meta";
const TOUCH: &str = "touch";
const PLAIN: &str = "plain";
const ESCALATE: &str = "escalate";
const PATHS: [&str; 6] = ["a.txt", "src/b.rs", "c/d/e.md", "f", "g.json", "h i.txt"];
const CALLS: Range<u64> = 20..400;
const PINNED_MODELS: [Model; 5] = [
    Model::Luna,
    Model::Sol,
    Model::Sol62,
    Model::Astra,
    Model::Astra62,
];
const UNSERVED_TEXT: [&str; 4] = [
    "# Plugins and Apps",
    "plugin://",
    "app://",
    "list_mcp_resources",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NestedMetaMutation {
    None,
    ForgetsWritten,
    WrittenOnFailure,
    CatalogKeepsEscalation,
    CallKeepsEscalation,
    UnservedSectionsKept,
}

impl NestedMetaMutation {
    const ALL: [Self; 6] = [
        Self::None,
        Self::ForgetsWritten,
        Self::WrittenOnFailure,
        Self::CatalogKeepsEscalation,
        Self::CallKeepsEscalation,
        Self::UnservedSectionsKept,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|mutation| mutation.name() == name)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ForgetsWritten => "forgets_written",
            Self::WrittenOnFailure => "written_on_failure",
            Self::CatalogKeepsEscalation => "catalog_keeps_escalation",
            Self::CallKeepsEscalation => "call_keeps_escalation",
            Self::UnservedSectionsKept => "unserved_sections_kept",
        }
    }

    pub fn names() -> String {
        Self::ALL[1..]
            .iter()
            .map(|mutation| mutation.name())
            .collect::<Vec<_>>()
            .join("|")
    }

    const fn rule(self) -> Rule {
        match self {
            Self::None => Rule::None,
            Self::ForgetsWritten => Rule::ForgetsWritten,
            Self::WrittenOnFailure => Rule::WrittenOnFailure,
            Self::CatalogKeepsEscalation => Rule::CatalogKeepsEscalation,
            Self::CallKeepsEscalation => Rule::CallKeepsEscalation,
            Self::UnservedSectionsKept => Rule::None,
        }
    }

    const fn prompt_rule(self) -> prompt_sim::Mutation {
        match self {
            Self::UnservedSectionsKept => prompt_sim::Mutation::UnservedSectionsKept,
            _ => prompt_sim::Mutation::None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NestedMetaStats {
    pub seeds: u64,
    pub calls: u64,
    pub wrote: u64,
    pub failed: u64,
    pub plain: u64,
    pub unescalated: u64,
    pub escalating: u64,
}

impl NestedMetaStats {
    pub const fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.calls += other.calls;
        self.wrote += other.wrote;
        self.failed += other.failed;
        self.plain += other.plain;
        self.unescalated += other.unescalated;
        self.escalating += other.escalating;
    }
}

struct Outcome {
    digest: u64,
    wrong: Option<String>,
    stats: NestedMetaStats,
    shape: String,
}

struct Touch;

impl nanocode::FunctionHandler for Touch {
    fn call<'a>(
        &'a self,
        _name: &'a str,
        input: Value,
        _context: nanocode::tools::ToolContext<'a>,
    ) -> nanocode::HostFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            if input.get("fail").and_then(Value::as_bool) == Some(true) {
                return Err("refused".to_owned());
            }
            Ok(json!({ "ok": true }))
        })
    }
}

struct Escalate(Arc<Slot<Vec<String>>>);

impl nanocode::FunctionHandler for Escalate {
    fn call<'a>(
        &'a self,
        _name: &'a str,
        input: Value,
        _context: nanocode::tools::ToolContext<'a>,
    ) -> nanocode::HostFuture<'a, Result<Value, String>> {
        let keys = input
            .as_object()
            .map(|fields| fields.keys().cloned().collect())
            .unwrap_or_default();
        drop(self.0.put(keys));
        Box::pin(async move { Ok(json!({ "ok": true })) })
    }
}

fn escalation_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "cmd": { "type": "string" },
            "sandbox_permissions": { "type": "string" },
            "justification": { "type": "string" },
            "prefix_rule": { "type": "array" },
            "dangerouslyDisableSandbox": { "type": "boolean" }
        },
        "required": ["cmd", "justification"]
    })
}

fn declared(definition: &nanocode_code_mode::ToolDefinition) -> Option<(Vec<String>, Vec<String>)> {
    let nanocode_code_mode::ToolDefinition::Function { parameters, .. } = definition else {
        return None;
    };
    let schema = parameters.as_value();
    let names = |key: &str| -> Vec<String> {
        match schema.get(key) {
            Some(Value::Object(fields)) => fields.keys().cloned().collect(),
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            _ => Vec::new(),
        }
    };
    Some((names("properties"), names("required")))
}

struct Declares;

impl CodeModeToolPolicy for Declares {
    fn file_access(&self, name: &str, input: &Value) -> Vec<FileAccess> {
        if name != TOUCH {
            return Vec::new();
        }
        input
            .get("files")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|file| {
                let path = file.get("p").and_then(Value::as_str)?.to_owned();
                Some(if file.get("w").and_then(Value::as_bool) == Some(true) {
                    FileAccess::Write(path)
                } else {
                    FileAccess::Read(path)
                })
            })
            .collect()
    }
}

fn tools(unescalated: bool, seen: &Arc<Slot<Vec<String>>>) -> Tools {
    let schema = json!({ "type": "object", "properties": { "files": { "type": "array" }, "fail": { "type": "boolean" } } });
    let builder = Tools::builder()
        .without_defaults()
        .tool(nanocode::FunctionTool::new(
            TOUCH,
            "Touch files",
            schema.clone(),
            Arc::new(Touch),
        ))
        .tool(nanocode::FunctionTool::new(
            PLAIN,
            "Touch nothing",
            schema,
            Arc::new(Touch),
        ))
        .tool(nanocode::FunctionTool::new(
            ESCALATE,
            "Run with escalation",
            escalation_schema(),
            Arc::new(Escalate(Arc::clone(seen))),
        ));
    let builder = if unescalated {
        builder.without_escalation()
    } else {
        builder
    };
    builder
        .build()
        .expect("the nested_meta tools build")
        .with_code_mode_policy(Arc::new(Declares))
}

fn written(output: &ToolOutput) -> Option<Value> {
    let raw = output.metadata.as_deref()?;
    serde_json::from_str::<Value>(raw.get())
        .ok()?
        .get("written")
        .cloned()
}

fn unserved_sections() -> Option<String> {
    let mut named = [false; UNSERVED_TEXT.len()];
    for model in PINNED_MODELS {
        let upstream = ModelConfig {
            model: model.clone(),
            codesign: false,
            ..ModelConfig::default()
        };
        let served = ModelConfig {
            model: model.clone(),
            ..ModelConfig::default()
        };
        let (upstream, served) = (upstream.base_system_prompt(), served.base_system_prompt());
        for (text, named) in UNSERVED_TEXT.into_iter().zip(&mut named) {
            *named |= upstream.contains(text);
            if served.contains(text) {
                return Some(format!(
                    "unserved_section: the {model:?} prompt still carries {text:?} though the SDK serves no plugins or apps"
                ));
            }
        }
    }
    UNSERVED_TEXT.into_iter().zip(named).find_map(|(text, named)| {
        (!named).then(|| {
            format!(
                "unserved_stale: no pinned prompt contains {text:?} any more, so stripping the unserved sections matches nothing"
            )
        })
    })
}

fn run_seed(seed: u64, mutation: NestedMetaMutation) -> Outcome {
    host_sim::mutate(mutation.rule());
    prompt_sim::mutate(mutation.prompt_rule());
    crate::host::enter(seed, &Clock::new());
    let mut rng = Rng::derive(seed, "nested_meta");
    let calls = rng.range(CALLS.start, CALLS.end);
    let shape = format!("calls={calls}");
    let mut esc = Rng::derive(seed, "nested_meta escalation calls");
    let unescalated = Rng::derive(seed, "nested_meta escalation").below(2) == 0;
    let seen = Arc::new(Slot::default());
    let runtime =
        ToolRuntime::new_with_tools(std::env::temp_dir(), Vec::new(), &tools(unescalated, &seen));
    let mut stats = NestedMetaStats {
        seeds: 1,
        calls,
        ..NestedMetaStats::default()
    };
    let mut digest = fnv(FNV_OFFSET, shape.as_bytes());
    let mut wrong = None;
    if unescalated {
        stats.unescalated += 1;
    }
    let catalogs = [
        ("model", runtime.model_specs(SESSION)),
        ("code_mode", runtime.registered_code_mode_definitions()),
    ];
    for (surface, definitions) in &catalogs {
        let Some((properties, required)) = definitions
            .iter()
            .find(|definition| definition.name() == ESCALATE)
            .and_then(declared)
        else {
            continue;
        };
        let listed: Vec<&str> = ESCALATION_INPUTS
            .iter()
            .copied()
            .filter(|field| {
                properties.iter().any(|name| name == field)
                    || required.iter().any(|name| name == field)
            })
            .collect();
        let expected = if unescalated {
            0
        } else {
            ESCALATION_INPUTS.len()
        };
        if listed.len() != expected || !properties.iter().any(|name| name == "cmd") {
            wrong = Some(format!(
                "catalog_escalation: {surface} catalog lists {listed:?} (unescalated={unescalated}) properties={properties:?}"
            ));
        }
        digest = fnv(
            digest,
            format!("{surface}:{properties:?}:{required:?}").as_bytes(),
        );
    }
    wrong = wrong.or_else(unserved_sections);
    for index in 0..calls {
        if wrong.is_some() {
            break;
        }
        if esc.below(4) == 0 {
            let mut input = serde_json::Map::new();
            input.insert("cmd".into(), json!("true"));
            let mut sent = Vec::new();
            for field in ESCALATION_INPUTS {
                if esc.below(2) == 0 {
                    input.insert(field.into(), json!("x"));
                    sent.push(field);
                }
            }
            let step = format!("esc-{index}");
            let context = ToolContext::new("", SESSION, &step, &[], usize::MAX);
            let output =
                block_on(runtime.execute_nested_tool(ESCALATE, Value::Object(input), context));
            let keys = seen.take().unwrap_or_default();
            digest = fnv(
                digest,
                format!("{step}:{}:{keys:?}", output.success).as_bytes(),
            );
            if !sent.is_empty() {
                stats.escalating += 1;
            }
            let arrived: Vec<&str> = ESCALATION_INPUTS
                .iter()
                .copied()
                .filter(|field| keys.iter().any(|key| key == field))
                .collect();
            let want: Vec<&str> = if unescalated {
                Vec::new()
            } else {
                sent.clone()
            };
            if !output.success || arrived != want || !keys.iter().any(|key| key == "cmd") {
                wrong = Some(format!(
                    "call_escalation: call {index} sent {sent:?} (unescalated={unescalated}), the tool saw {keys:?}, success={}",
                    output.success
                ));
                break;
            }
        }
        let name = if rng.below(5) == 0 { PLAIN } else { TOUCH };
        let fail = rng.below(4) == 0;
        let count = rng.below(5);
        let files: Vec<Value> = (0..count)
            .map(|_| json!({ "p": PATHS[rng.pick(PATHS.len())], "w": rng.below(2) == 0 }))
            .collect();
        let expected: BTreeSet<&str> = files
            .iter()
            .filter(|file| file["w"] == json!(true))
            .filter_map(|file| file["p"].as_str())
            .collect();
        let step = format!("step-{index}");
        let context = ToolContext::new("", SESSION, &step, &[], usize::MAX);
        let output = block_on(runtime.execute_nested_tool(
            name,
            json!({ "files": files, "fail": fail }),
            context,
        ));
        let reported = written(&output);
        digest = fnv(digest, step.as_bytes());
        digest = fnv(digest, &[u8::from(output.success)]);
        digest = fnv(
            digest,
            reported
                .as_ref()
                .map_or_else(String::new, Value::to_string)
                .as_bytes(),
        );
        if output.success == fail {
            wrong = Some(format!(
                "call {index}: {name} fail={fail} returned success={}",
                output.success
            ));
            break;
        }
        let want = (name == TOUCH && !fail && !expected.is_empty())
            .then(|| json!(expected.iter().collect::<Vec<_>>()));
        if name == PLAIN {
            stats.plain += 1;
        } else if fail {
            stats.failed += 1;
        } else if want.is_some() {
            stats.wrote += 1;
        }
        if reported != want {
            wrong = Some(format!(
                "written_reported: call {index} {name} fail={fail} files={files:?} reported {reported:?}, expected {want:?}"
            ));
            break;
        }
    }
    host_sim::mutate(Rule::None);
    prompt_sim::mutate(prompt_sim::Mutation::None);
    Outcome {
        digest,
        wrong,
        stats,
        shape,
    }
}

pub fn judge(
    seed: u64,
    mutation: NestedMetaMutation,
    twice: bool,
    verbose: bool,
    totals: &mut NestedMetaStats,
) -> Option<String> {
    let first = run_seed(seed, mutation);
    totals.add(&first.stats);
    if verbose {
        println!(
            "seed {seed} digest {:016x} {} stats={:?}",
            first.digest, first.shape, first.stats
        );
    }
    if first.wrong.is_some() || !twice {
        return first
            .wrong
            .map(|message| format!("{} :: {message}", first.shape));
    }
    let second = run_seed(seed, mutation);
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "{} :: nondeterminism: digest {:016x} then {:016x}",
            first.shape, first.digest, second.digest
        )
    })
}

pub fn replay(seed: u64, mutation: NestedMetaMutation) -> String {
    let mut command = format!("nanocode-sim --scenario nested_meta --seed {seed}");
    if mutation != NestedMetaMutation::None {
        command.push_str(" --mutation ");
        command.push_str(mutation.name());
    }
    command
}

pub fn report(
    seeds: &Range<u64>,
    passed: u64,
    failed: u64,
    twice: bool,
    mutation: NestedMetaMutation,
    totals: &NestedMetaStats,
) -> ExitCode {
    println!(
        "RESULT seeds={}..{} scenario=nested_meta mutation={} passed={passed} failed={failed} twice={twice} stats={totals:?}",
        seeds.start,
        seeds.end,
        mutation.name()
    );
    match mutation {
        NestedMetaMutation::None if failed == 0 => ExitCode::SUCCESS,
        NestedMetaMutation::None => ExitCode::FAILURE,
        _ if failed > 0 => {
            println!("MUTATION {} caught by {failed} seeds", mutation.name());
            ExitCode::SUCCESS
        }
        _ => {
            println!("MUTATION {} NOT caught", mutation.name());
            ExitCode::FAILURE
        }
    }
}
