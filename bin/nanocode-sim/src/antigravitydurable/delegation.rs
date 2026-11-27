use super::*;

const CHILD: &str = "GEMINI_CHILD_TASK";
const OUTPUT: &str = "result from the Gemini child";
const SIDE: &str = "GEMINI_SIDE_QUESTION";

#[derive(Default)]
struct DelegationWire {
    calls: AtomicU64,
    compacting: AtomicBool,
    compacted: AtomicU64,
    child: Atom<Option<String>>,
    grounded: AtomicBool,
    wrong: Atom<Vec<String>>,
}

struct DelegationSender(Arc<DelegationWire>);

fn call(name: &str, args: Value) -> Value {
    json!({"functionCall": {"name": name, "args": args}})
}

impl DelegationSender {
    fn reply(
        &self,
        request: nanocode_http::layer::Request,
    ) -> Result<nanocode_http::layer::Response, String> {
        let body: Value =
            serde_json::from_slice(&request.body).map_err(|error| error.to_string())?;
        let streaming = !request.url.contains(":generateContent");
        let wire = &self.0;
        let parts = if wire.compacting.load(Ordering::Acquire) {
            wire.compacted.fetch_add(1, Ordering::AcqRel);
            wire.check_compaction();
            vec![json!({"text": "Compaction summary"})]
        } else if !streaming {
            let child = wire.child.get();
            let session = child
                .as_deref()
                .ok_or("grounding ran before the child streamed")?;
            wire.check(session, Some(TURN));
            wire.grounded.store(true, Ordering::Release);
            vec![json!({"text": "Grounding answer"})]
        } else {
            let session = body["labels"]["cascade_id"]
                .as_str()
                .ok_or("request lacked a cascade ID")?;
            let contents = body["contents"]
                .as_array()
                .ok_or("request lacked contents")?;
            let first = contents
                .first()
                .ok_or("request had no contents")?
                .to_string();
            let last = contents
                .last()
                .ok_or("request had no contents")?
                .to_string();
            let side = last.contains(SIDE);
            wire.check(session, (!side).then_some(TURN));
            if side {
                vec![json!({"text": SIDE})]
            } else if first.contains(CHILD) {
                if session == SESSION {
                    return Err("the child used the parent session".to_owned());
                }
                if contents.len() == 1 {
                    wire.child.set(Some(session.to_owned()));
                    vec![
                        call("search_web", json!({"query": "Find a simulated result"})),
                        call(
                            "exec",
                            json!({"code": format!(
                                "await tools.check_host({{}}); text(JSON.stringify(await tools.submit_result({{output: {OUTPUT:?}}})));"
                            )}),
                        ),
                    ]
                } else {
                    vec![json!({"text": "Child finished"})]
                }
            } else if session != SESSION {
                return Err(format!("the parent used session {session:?}"));
            } else if contents.len() == 1 {
                vec![call(
                    "exec",
                    json!({"code": format!(
                        "const child = await tools.spawn_agent({{role: 'worker', task: {CHILD:?}, harness: null, model: null, thinking: null, reasoning_mode: null, output_contract: {{kind: 'string'}}}}); const waited = await tools.wait_agent({{agent_ids: [child.agent_id], timeout_ms: {}}}); text(JSON.stringify(waited.agents[0].status));",
                        crate::server::WAIT_AGENT_TIMEOUT_MS,
                    )}),
                )]
            } else if let Some(cell) = running_cell(&last) {
                vec![call("wait", json!({"cell_id": cell}))]
            } else if last.contains(OUTPUT) {
                vec![json!({"text": OUTPUT})]
            } else {
                vec![json!({"text": format!("parent did not receive the child result: {last}")})]
            }
        };
        Ok(model_response(parts, streaming))
    }
}

fn running_cell(last: &str) -> Option<&str> {
    let (_, rest) = last.split_once("Script running with cell ID ")?;
    rest.split(|ch: char| !ch.is_ascii_alphanumeric()).next()
}

impl DelegationWire {
    fn check_compaction(&self) {
        use nanocode_http::layer::origin;
        let (session, context) = (origin::session(), origin::current());
        let operation = context
            .as_deref()
            .is_some_and(|context| context.starts_with("antigravity-compact-"));
        if session.as_deref() != Some(SESSION) || !operation {
            self.wrong.update(|wrong| {
                wrong.push(format!(
                    "compaction HTTP origin named session {session:?}, context {context:?}; expected {SESSION:?} and the compaction operation ID"
                ));
            });
        }
    }

    fn check(&self, session: &str, context: Option<&str>) {
        if let Err(error) = check_origin(session, context) {
            self.wrong.update(|wrong| wrong.push(error.clone()));
        }
    }
}

impl nanocode_http::layer::Sender for DelegationSender {
    fn send(
        &self,
        request: nanocode_http::layer::Request,
    ) -> nanocode_http::layer::BoxFuture<
        '_,
        Result<nanocode_http::layer::Response, nanocode_http::layer::Error>,
    > {
        Box::pin(async move {
            self.0.calls.fetch_add(1, Ordering::AcqRel);
            self.reply(request)
                .map_err(nanocode_http::layer::Error::new)
        })
    }
}

struct CheckHost(Arc<DelegationWire>);

impl nanocode::FunctionHandler for CheckHost {
    fn call<'a>(
        &'a self,
        _name: &'a str,
        _input: Value,
        context: nanocode::tools::ToolContext<'a>,
    ) -> nanocode::HostFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            if context.host_context() != Some(TURN) {
                self.0.wrong.update(|wrong| {
                    wrong.push(format!(
                        "child tool host context was {:?}",
                        context.host_context()
                    ));
                });
            }
            Ok(Value::Null)
        })
    }
}

async fn run(wire: Arc<DelegationWire>, media: &Media) -> Result<(), String> {
    let (store, _) = media.open(Store::Kv).await?;
    let tools = Tools::builder()
        .without_defaults()
        .tool(nanocode::FunctionTool::new(
            "check_host",
            "Read the simulated tool's host context",
            json!({"type": "object", "properties": {}}),
            Arc::new(CheckHost(Arc::clone(&wire))),
        ))
        .build()
        .map_err(|error| error.to_string())?;
    let runtime = runtime(Arc::new(DelegationSender(Arc::clone(&wire))), store, tools)?;
    let (parent, _events) = runtime
        .spawn()
        .session_id(SESSION)
        .build()
        .await
        .map_err(|error| error.to_string())?;
    let result = parent
        .prompt(PromptRequest::new("Delegate to a Gemini child").request_id(TURN))
        .await
        .map_err(|error| error.to_string())?
        .result()
        .await
        .map_err(|error| error.to_string())?;
    wire.compacting.store(true, Ordering::Release);
    let compacted = parent.compact().await;
    wire.compacting.store(false, Ordering::Release);
    compacted.map_err(|error| format!("compact: {error}"))?;
    if wire.compacted.load(Ordering::Acquire) != 1 {
        return Err("the compaction made no model request".to_owned());
    }
    let side = parent
        .side_question(SIDE)
        .await
        .map_err(|error| error.to_string())?;
    if side.text() != SIDE {
        return Err(format!("the side question answered {:?}", side.text()));
    }
    if !wire.grounded.load(Ordering::Acquire) {
        return Err("the child did not make a grounding request".to_owned());
    }
    if result.final_message != OUTPUT {
        return Err(result.final_message);
    }
    Ok(())
}

pub(super) fn simulate(seed: u64) -> (u64, Vec<String>) {
    let clock = Clock::new();
    crate::host::enter(seed, &clock);
    let profile = Profile {
        before_ppm: 0,
        after_ppm: 0,
        torn_ppm: 0,
        read_ppm: 0,
        durability: Durability::OnAck,
        ..Profile::of(seed)
    };
    let media = Media {
        kv: SimKv::new(
            seed,
            profile,
            StoreMutation::None,
            false,
            Arc::clone(&clock),
        ),
        disk: Disk::new(),
    };
    let wire = Arc::new(DelegationWire::default());
    let (ended, result) = epoch(
        seed,
        &clock,
        "antigravity delegation",
        run(Arc::clone(&wire), &media),
    );
    let mut wrong = wire.wrong.get().as_ref().clone();
    match (ended, result) {
        (Ended::Finished, Some(Ok(()))) => {}
        (Ended::Finished, Some(Err(error))) => wrong.push(error),
        (ended, _) => wrong.push(format!("delegation ended {ended:?} without a result")),
    }
    (wire.calls.load(Ordering::Acquire), wrong)
}
