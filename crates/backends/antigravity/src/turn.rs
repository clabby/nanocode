use crate::driver::{Inner, Progress, State, Turn, emit, prompt_text, response};
use crate::profile::{Profile, SKIP_SIGNATURE, Thinking as ProfileThinking};
use crate::prompts;
use crate::request::Request;
use nanocode_agent_core::{
    BackendError, BackendFailure, CostStatus, NanocodeError, ReportedTurnUsage, Result, SideAnswer,
    TurnResult, TurnUsage,
    events::{AgentEventKind, AgentEventPublisher},
    input::{Prompt, PromptMessageRole},
};
use nanocode_backend_antigravity_client::{ClientError, Content, FunctionCall, Part, Reply};
use nanocode_backend_antigravity_tools::{
    NAMES, Outcome, REMINDER, local_timestamp, system_message, with_header,
};
use nanocode_durability::{BeginStep, EncodedPayload, OwnerWrite, StepRecovery, agent_error};
use nanocode_http::layer::origin::Origin;
use nanocode_tool_host::ToolContext;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::Ordering;

const MODEL_STEP: &str = "model_call";
pub(crate) const MOVED: &str = "The session moved to another server while this call ran, so its outcome is unknown. Check what it changed before you run it again.";
const RUNNING: &str =
    "This tool call is still running in the main conversation; its result is not available yet.";
pub(crate) const INTERRUPTED: &str =
    "Tool execution interrupted; outcome unknown. Check what it changed before you run it again.";

#[derive(Debug)]
pub(crate) struct ModelFailure {
    message: String,
    retryable: bool,
}

impl std::fmt::Display for ModelFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ModelFailure {}

impl BackendFailure for ModelFailure {
    fn retryable(&self) -> bool {
        self.retryable
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn model_error(error: &ClientError) -> NanocodeError {
    NanocodeError::Response(BackendError(Box::new(ModelFailure {
        message: error.to_string(),
        retryable: error.retryable(),
    })))
}

pub(crate) fn normalize(parts: Vec<Part>, skip: bool) -> Vec<Part> {
    let mut carry = None;
    let mut first_call = true;
    let mut out = Vec::with_capacity(parts.len());
    for mut part in parts {
        if part.is_thought() {
            if part.thought_signature.is_some() {
                carry = part.thought_signature.take();
            }
            out.push(part);
            continue;
        }
        if part.thought_signature.is_none() {
            part.thought_signature = carry.take();
        }
        if part.function_call.is_some() {
            if first_call && skip && part.thought_signature.is_none() {
                part.thought_signature = Some(SKIP_SIGNATURE.to_owned());
            }
            first_call = false;
        }
        out.push(part);
    }
    out
}

pub(crate) fn visible(parts: &[Part]) -> String {
    parts
        .iter()
        .filter(|part| !part.is_thought())
        .filter_map(|part| part.text.as_deref())
        .collect::<Vec<_>>()
        .join("\n")
}

fn append(part: &mut Part, text: &str) {
    if let Some(response) = part.function_response.as_mut()
        && let Some(map) = response.response.as_object_mut()
        && let Some((_, Value::String(body))) = map.iter_mut().next()
    {
        body.push_str("\n\n");
        body.push_str(text);
    }
}

fn passive(call: &FunctionCall) -> bool {
    match call.name.as_str() {
        "schedule" => true,
        "manage_task" => call.args.get("Action").and_then(Value::as_str) != Some("list"),
        _ => false,
    }
}

pub(crate) fn effort(profile: &Profile) -> String {
    match profile.thinking {
        ProfileThinking::Level(level) => level.to_ascii_lowercase(),
        ProfileThinking::Budget(budget) => budget.to_string(),
    }
}

pub(crate) fn reported(usage: crate::driver::Usage) -> TurnUsage {
    TurnUsage::from_reported(ReportedTurnUsage {
        input_tokens: usage.input,
        cached_input_tokens: usage.cached,
        cache_write_input_tokens: 0,
        output_tokens: usage.output,
        reasoning_output_tokens: usage.reasoning,
        total_tokens: usage.total,
        estimated_cost: None,
        cost_status: CostStatus::UsageNotReported,
    })
}

enum Phase {
    Calls,
    Answered,
    Request,
}

impl State {
    fn phase(&self) -> Phase {
        match self.contents.last() {
            Some(content) if content.role == "model" && content.calls().next().is_some() => {
                Phase::Calls
            }
            Some(content) if content.role == "model" => Phase::Answered,
            _ => Phase::Request,
        }
    }

    pub(crate) fn open_ids(&self, turn: &str) -> Vec<String> {
        if !matches!(self.phase(), Phase::Calls) {
            return Vec::new();
        }
        let Some(last) = self.contents.last() else {
            return Vec::new();
        };
        let mut ids = crate::record::call_ids(turn, self.calls.saturating_sub(1), &last.parts);
        ids.drain(..self.progress.responses.len().min(ids.len()));
        ids
    }

    pub(crate) fn close_calls(&mut self, answer: &str) {
        if !matches!(self.phase(), Phase::Calls) {
            return;
        }
        let Some(last) = self.contents.last() else {
            return;
        };
        let calls = last.calls().cloned().collect::<Vec<_>>();
        let mut responses = std::mem::take(&mut self.progress.responses);
        for call in calls.iter().skip(responses.len()) {
            responses.push(response(call, "error", answer.to_owned()));
        }
        self.push(Content::user(responses));
    }
}

impl Inner {
    pub(crate) fn origin(&self, context: Option<Arc<str>>) -> Origin {
        if crate::sim::mutated(crate::sim::Mutation::OriginMissing) {
            return Origin::default();
        }
        Origin {
            context: self.config.host_context.clone().or(context),
            session: Some(Arc::from(self.config.session.as_str())),
        }
    }

    pub(crate) fn compaction_origin(&self, context: Option<Arc<str>>) -> Origin {
        if crate::sim::mutated(crate::sim::Mutation::CompactionOriginMissing) {
            return Origin::default();
        }
        self.origin(context)
    }

    pub(crate) async fn request(
        &self,
        state: &State,
        extra: Option<&Content>,
        max_output_tokens: u32,
        call: u64,
        events: &AgentEventPublisher,
    ) -> Result<Reply> {
        let config = &self.config;
        let profile = self.profile.get();
        let cached = match &config.cache {
            Some(cache) => {
                cache
                    .resolve(
                        &config.client,
                        &profile.model,
                        &config.prefix.system,
                        &config.prefix.tools,
                    )
                    .await
            }
            None => None,
        };
        let body = Request {
            contents: &state.contents,
            extra,
            prefix: &config.prefix,
            cached: cached.as_deref(),
            profile: &profile,
            ids: &config.ids,
            step: state.step,
            call,
            max_output_tokens,
            execution: state.last_execution.as_deref(),
            labels: !config.client.endpoint().route.is_vertex(),
        }
        .encode();
        emit(
            events,
            AgentEventKind::ModelCallStarted,
            json!({
                "call_index": call,
                "model_call_index": call,
                "model": profile.model,
                "reasoning_mode": "standard",
                "effort": effort(&profile),
            }),
        );
        let started = nanocode_platform::now();
        let nanos = |since: nanocode_platform::Instant| {
            u64::try_from(nanocode_platform::elapsed(since).as_nanos()).unwrap_or(u64::MAX)
        };
        let mut first_event = None;
        let mut first_output = None;
        let mut on_part = |part: &Part| {
            first_event.get_or_insert_with(|| nanos(started));
            if let Some(text) = part.text.as_deref().filter(|_| !part.is_thought()) {
                first_output.get_or_insert_with(|| nanos(started));
                emit(
                    events,
                    AgentEventKind::AssistantDelta,
                    json!({"model_call_index": call, "item_id": format!("call-{call}"), "text": text}),
                );
            }
        };
        let reply = config
            .client
            .stream(&profile.model, &body, &mut on_part)
            .await;
        let reply = match reply {
            Ok(reply) => reply,
            Err(error) => {
                emit(
                    events,
                    AgentEventKind::ModelCallFailed,
                    json!({
                        "call_index": call,
                        "model_call_index": call,
                        "model": profile.model,
                        "duration_ns": nanos(started),
                        "error": error.to_string(),
                    }),
                );
                return Err(model_error(&error));
            }
        };
        let usage = reply.usage;
        let output_tokens = usage.candidates_token_count + usage.thoughts_token_count;
        emit(
            events,
            AgentEventKind::ModelCallCompleted,
            json!({
                "call_index": call,
                "model_call_index": call,
                "model": profile.model,
                "attempt": 1,
                "connection_generation": 0,
                "status": reply
                    .finish_reason
                    .as_deref()
                    .unwrap_or("STOP")
                    .to_ascii_lowercase(),
                "duration_ns": nanos(started),
                "time_to_first_event_ns": first_event.unwrap_or_else(|| nanos(started)),
                "time_to_first_output_ns": first_output,
                "tool_calls": reply.parts.iter().filter(|part| part.function_call.is_some()).count(),
                "usage": {
                    "input_tokens": usage.prompt_token_count,
                    "input_tokens_details": {
                        "cached_tokens": usage.cached_content_token_count,
                        "cache_write_tokens": 0,
                    },
                    "output_tokens": output_tokens,
                    "output_tokens_details": {"reasoning_tokens": usage.thoughts_token_count},
                    "total_tokens": usage.total_token_count.max(usage.prompt_token_count + output_tokens),
                },
            }),
        );
        self.log_usage(call, &profile.model, usage);
        Ok(reply)
    }

    fn log_usage(&self, call: u64, model: &str, usage: nanocode_backend_antigravity_client::Usage) {
        use std::io::Write as _;
        let Some(logs) = &self.config.logs else {
            return;
        };
        let line = json!({
            "call_index": call,
            "model": model,
            "prompt_tokens": usage.prompt_token_count,
            "cache_read_tokens": usage.cached_content_token_count,
            "output_tokens": usage.candidates_token_count,
            "thoughts_tokens": usage.thoughts_token_count,
            "total_tokens": usage.total_token_count,
        });
        let _ = std::fs::create_dir_all(logs);
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(logs.join(crate::driver::USAGE_LOG))
        {
            let _ = writeln!(file, "{line}");
        }
    }

    fn write_transcript(&self, state: &State) {
        let Some(logs) = &self.config.logs else {
            return;
        };
        let _ = std::fs::create_dir_all(logs);
        let mut full = String::new();
        let mut compact = String::new();
        for (index, content) in state.transcript.iter().enumerate() {
            let line = content.to_json().encode();
            full.push_str(&line);
            full.push('\n');
            let mut truncated = Vec::new();
            let mut short = content.clone();
            for part in &mut short.parts {
                if let Some(text) = part.text.as_mut()
                    && text.len() > 2_000
                {
                    let mut cut = 2_000;
                    while !text.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    text.truncate(cut);
                    truncated.push("text");
                }
                if let Some(response) = part.function_response.as_mut()
                    && let Some(map) = response.response.as_object_mut()
                {
                    for (key, value) in map.iter_mut() {
                        if let Value::String(body) = value
                            && body.len() > 2_000
                        {
                            let mut cut = 2_000;
                            while !body.is_char_boundary(cut) {
                                cut -= 1;
                            }
                            body.truncate(cut);
                            truncated.push(if key == "error" { "error" } else { "output" });
                        }
                    }
                }
            }
            let mut value: Value =
                serde_json::from_str(&short.to_json().encode()).unwrap_or(Value::Null);
            if let Some(map) = value.as_object_mut() {
                map.insert("step".into(), json!(index));
                if !truncated.is_empty() {
                    map.insert("truncated_fields".into(), json!(truncated));
                }
            }
            compact.push_str(&value.to_string());
            compact.push('\n');
        }
        let _ = std::fs::write(logs.join("transcript_full.jsonl"), full);
        let _ = std::fs::write(logs.join("transcript.jsonl"), compact);
    }

    pub(crate) async fn compact(
        &self,
        state: &mut State,
        events: &AgentEventPublisher,
    ) -> Result<()> {
        emit(events, AgentEventKind::ModelCompactionStarted, json!({}));
        self.write_transcript(state);
        let extra = Content::user(vec![Part::text(prompts::COMPACTION)]);
        let output_tokens = self.profile.get().compaction_output_tokens;
        let reply = self
            .request(state, Some(&extra), output_tokens, state.calls, events)
            .await;
        let reply = match reply {
            Ok(reply) => reply,
            Err(error) => {
                emit(
                    events,
                    AgentEventKind::ModelCompactionFailed,
                    json!({"error": error.to_string()}),
                );
                return Err(error);
            }
        };
        let summary = visible(&reply.parts);
        let keep = self.profile.get().max_user_requests;
        let requests = state.requests[state.requests.len().saturating_sub(keep)..].to_vec();
        let text = prompts::context_summary(
            &local_timestamp(Self::now()),
            &requests,
            summary.trim(),
            &self.config.engine.snapshots(),
            self.config.logs.is_some(),
        );
        state.contents = vec![Content::user(vec![Part::text(text.clone())])];
        state.transcript.push(Content::user(vec![Part::text(text)]));
        state.step += 1;
        state.prompt_tokens = 0;
        state.streak = 0;
        emit(events, AgentEventKind::ModelCompactionCompleted, json!({}));
        Ok(())
    }

    fn context<'a>(
        &'a self,
        call_id: &'a str,
        turn: &'a Turn,
        instruction_revision: Option<u64>,
    ) -> ToolContext<'a> {
        let revision = instruction_revision
            .filter(|_| !crate::sim::mutated(crate::sim::Mutation::RevisionMissing));
        let host_context = (!crate::sim::mutated(crate::sim::Mutation::HostContextMissing))
            .then_some(&*turn.host_context);
        ToolContext::new(
            self.config.model.as_str(),
            &self.config.session,
            call_id,
            &[],
            usize::MAX,
        )
        .with_turn_id(Some(&turn.id))
        .with_instruction_revision(revision)
        .with_host_context(host_context)
    }

    async fn execute(
        &self,
        name: &str,
        args: &Value,
        step: u64,
        context: ToolContext<'_>,
    ) -> (&'static str, String) {
        let created = Self::now();
        let engine = &self.config.engine;
        if NAMES.contains(&name) {
            return match engine.call(name, args, step).await {
                Outcome::Output(body) => {
                    let full = with_header(created, Self::now(), &body);
                    ("output", engine.finish_output(name, step, &full).await)
                }
                Outcome::Background(text) => {
                    let _ = engine.finish_output("read_file", step, &text).await;
                    ("output", text)
                }
                Outcome::Error(error) => ("error", error),
            };
        }
        if !self.config.nano.handles(name) {
            return (
                "error",
                format!("Encountered error in tool validation: tool {name:?} not found"),
            );
        }
        match self.config.nano.call(name, args, context).await {
            Ok(body) => {
                let full = with_header(created, Self::now(), &body);
                ("output", engine.finish_output(name, step, &full).await)
            }
            Err(error) => (
                "error",
                format!("Encountered error in tool execution: {error}"),
            ),
        }
    }

    async fn recover(&self, name: &str, context: ToolContext<'_>) -> (&'static str, String) {
        if self.config.nano.handles(name)
            && let Some(recovered) = self.config.nano.recover(name, context).await
        {
            return match recovered {
                Ok(body) => ("output", body),
                Err(error) => (
                    "error",
                    format!("Encountered error in tool execution: {error}"),
                ),
            };
        }
        ("error", MOVED.to_owned())
    }

    fn finish_execution(&self, state: &mut State) {
        state.executions += 1;
        state.last_execution = Some(crate::harness::new_id(&format!(
            "{}-execution-{}-{:?}",
            self.config.session,
            state.executions,
            Self::now()
        )));
    }

    fn deliver_idle(&self, state: &mut State) -> bool {
        let messages = self.config.engine.take_messages();
        let delivered = !messages.is_empty();
        for message in messages {
            state.step += 1;
            state.push(Content::user(vec![Part::text(system_message(
                &message.render(),
            ))]));
        }
        delivered
    }

    fn notice(&self, events: &AgentEventPublisher) {
        if let Some(notice) = &self.config.notice
            && !self.noticed.swap(true, Ordering::SeqCst)
        {
            emit(
                events,
                AgentEventKind::RunError,
                json!({"error": notice, "source": "antigravity", "observational": true}),
            );
        }
    }

    fn open(&self, state: &mut State, prompt: &Prompt, id: &str) {
        if state.contents.is_empty() {
            for message in prompt.transcript() {
                let model = message.role() == PromptMessageRole::Assistant;
                state.push(match model {
                    true => Content::model(vec![Part::text(message.content().to_owned())]),
                    false => Content::user(vec![Part::text(message.content().to_owned())]),
                });
            }
        }
        let text = prompt_text(&prompt.instruction);
        if !state.contents.is_empty() {
            state.step += 1;
        }
        let profile = self.profile.get();
        let settings = (!state.settings_sent).then(|| profile.display.clone());
        state.settings_sent = true;
        state.requests.push(text.clone());
        state.push(Content::user(vec![Part::text(prompts::user_request(
            &text,
            &local_timestamp(Self::now()),
            settings.as_deref(),
        ))]));
        state.progress = Progress {
            instruction_revision: prompt.instruction_revision(),
            ..Progress::default()
        };
        state.operation = self.config.durable.as_ref().map(|_| id.to_owned());
    }

    pub(crate) async fn turn(
        &self,
        mut state: State,
        prompt: &Prompt,
        turn: &Turn,
        id: &str,
    ) -> Result<TurnResult> {
        let events = &turn.events;
        self.notice(events);
        let mut recovering = state.operation.as_deref() == Some(id);
        if !recovering {
            self.open(&mut state, prompt, id);
            self.persist(&state).await?;
        } else if crate::sim::mutated(crate::sim::Mutation::RevisionForgotten) {
            state.progress.instruction_revision = None;
        }
        self.retain_steers(&mut state, turn).await?;
        loop {
            self.rounds(&mut state, turn, recovering).await?;
            recovering = false;
            turn.accepting.store(false, Ordering::SeqCst);
            std::sync::atomic::fence(Ordering::SeqCst);
            if turn.steers.get().is_empty() {
                break;
            }
            turn.accepting.store(true, Ordering::SeqCst);
        }
        self.finish_execution(&mut state);
        self.save(&state);
        self.write_transcript(&state);
        Ok(TurnResult {
            request_id: Some(id.to_owned()),
            turn_id: Some(id.to_owned()),
            final_message: state.progress.message.clone(),
            usage: Some(reported(state.progress.usage)),
            boundary: None,
        })
    }

    async fn rounds(&self, state: &mut State, turn: &Turn, mut recovering: bool) -> Result<()> {
        let events = &turn.events;
        let mut resumed = recovering;
        loop {
            match state.phase() {
                Phase::Calls => self.calls(state, turn, recovering).await?,
                Phase::Answered if recovering && turn.steers.get().is_empty() => return Ok(()),
                Phase::Answered | Phase::Request => {}
            }
            recovering = false;
            if state.prompt_tokens >= self.compaction_limit() {
                self.compact(state, events).await?;
                self.persist(state).await?;
            }
            let binds = self.inject_steers(state, turn);
            let called = self.reply(state, turn, &binds, !resumed).await?;
            resumed = false;
            turn.calls.store(state.progress.calls, Ordering::SeqCst);
            if called || !turn.steers.get().is_empty() {
                continue;
            }
            let engine = &self.config.engine;
            if !engine.has_urgent() && !engine.blocking() {
                return Ok(());
            }
            engine.wait().await;
            self.finish_execution(state);
            if !self.deliver_idle(state) {
                if turn.steers.get().is_empty() {
                    return Ok(());
                }
                continue;
            }
            if state.streak >= 2 {
                state.streak = 0;
                state.step += 1;
                state.push(Content::user(vec![Part::text(system_message(REMINDER))]));
            }
            self.persist(state).await?;
        }
    }

    async fn begin_model(
        &self,
        state: &State,
        step: &str,
        index: u32,
        advance: bool,
    ) -> Result<Option<Reply>> {
        self.save(state);
        let (Some(owner), Some(operation)) = (&self.config.durable, &state.operation) else {
            return Ok(None);
        };
        let input = EncodedPayload::encode(&json!({"call": index})).map_err(agent_error)?;
        let admit = |writes| {
            owner.admit_step(
                operation.clone(),
                writes,
                step.to_owned(),
                MODEL_STEP.to_owned(),
                input,
                StepRecovery::Rerun,
            )
        };
        let begun = match advance {
            true => {
                self.write(state, |continuation| {
                    admit(vec![OwnerWrite::Advance { continuation }])
                })
                .await?
            }
            false => admit(Vec::new()).await,
        }
        .map_err(agent_error)?;
        let BeginStep::Replay(output) = begun else {
            return Ok(None);
        };
        let output: Value = owner
            .load_payload(output)
            .await
            .map_err(agent_error)?
            .decode()
            .map_err(agent_error)?;
        let parts = serde_json::from_value(output["parts"].clone())
            .map_err(|error| NanocodeError::InvalidCheckpoint(error.to_string()))?;
        let usage = serde_json::from_value(output["usage"].clone())
            .map_err(|error| NanocodeError::InvalidCheckpoint(error.to_string()))?;
        Ok(Some(Reply {
            parts,
            usage,
            ..Reply::default()
        }))
    }

    async fn finish_model(&self, state: &State, step: &str, output: &Value) -> Result<()> {
        self.save(state);
        let (Some(owner), Some(operation)) = (&self.config.durable, &state.operation) else {
            return Ok(());
        };
        let output = EncodedPayload::encode(output).map_err(agent_error)?;
        self.write(state, |continuation| {
            owner.commit_writes(
                operation.clone(),
                vec![
                    OwnerWrite::CompleteStep {
                        step_id: step.to_owned(),
                        output,
                    },
                    OwnerWrite::Advance { continuation },
                ],
            )
        })
        .await?
        .map(drop)
        .map_err(agent_error)
    }

    async fn reply(
        &self,
        state: &mut State,
        turn: &Turn,
        binds: &[u32],
        advance: bool,
    ) -> Result<bool> {
        let events = &turn.events;
        let index = state.progress.calls + 1;
        let step = format!("model-{index}");
        let replayed = self.begin_model(state, &step, index, advance).await?;
        self.bind_steers(turn, binds, index).await?;
        let call_index = state.calls;
        let recorded = replayed.is_some() && self.recorded(&turn.id, index).await?;
        if !recorded {
            self.sync_facts(&state.contents).await?;
        }
        let reply = match replayed {
            Some(reply) => reply,
            None => {
                let max_output_tokens = self.profile.get().max_output_tokens;
                self.request(state, None, max_output_tokens, call_index, events)
                    .await?
            }
        };
        state.calls += 1;
        state.progress.calls = index;
        let usage = &mut state.progress.usage;
        usage.input += reply.usage.prompt_token_count;
        usage.cached += reply.usage.cached_content_token_count;
        usage.output += reply.usage.candidates_token_count + reply.usage.thoughts_token_count;
        usage.reasoning += reply.usage.thoughts_token_count;
        usage.total += reply.usage.total_token_count;
        state.prompt_tokens = reply.usage.prompt_token_count;
        state.step += 1;
        let parts = normalize(reply.parts, self.profile.get().skip_signature);
        let message = visible(&parts);
        if !message.is_empty() {
            state.progress.message = message.clone();
            emit(
                events,
                AgentEventKind::AssistantMessage,
                json!({"model_call_index": call_index, "item_id": format!("call-{call_index}"), "text": message}),
            );
        }
        let called = parts.iter().any(|part| part.function_call.is_some());
        if !called {
            state.streak += 1;
        }
        if !recorded {
            let ids = crate::record::call_ids(&turn.id, call_index, &parts);
            self.fact_model(&turn.id, index, &parts, &ids).await?;
        }
        let output = json!({"parts": parts, "usage": reply.usage});
        state.push(Content::model(parts));
        self.finish_model(state, &step, &output).await?;
        Ok(called)
    }

    async fn calls(&self, state: &mut State, turn: &Turn, recovering: bool) -> Result<()> {
        let events = &turn.events;
        let id = &turn.id;
        let calls = state
            .contents
            .last()
            .map(|content| content.calls().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let call_index = state.calls.saturating_sub(1);
        let start = state.progress.responses.len();
        let ids = state
            .contents
            .last()
            .map(|content| crate::record::call_ids(id, call_index, &content.parts))
            .unwrap_or_default();
        for (index, call) in calls.iter().enumerate().skip(start) {
            state.step += 1;
            let call_id = ids[index].clone();
            let recover = recovering && index == start;
            let settled = match recover {
                true => self.settled_result(&call_id).await?,
                false => None,
            };
            let unstarted =
                recover && settled.is_none() && self.intended(&call_id).await? == Some(false);
            let context = self.context(&call_id, turn, state.progress.instruction_revision);
            let (key, body) = match settled {
                Some(settled) => settled,
                None if recover && !unstarted => self.recover(&call.name, context).await,
                None => {
                    emit(
                        events,
                        AgentEventKind::ToolCall,
                        json!({"call_id": call_id, "tool": call.name, "arguments": call.args, "model_call_index": call_index}),
                    );
                    self.fact_intent(id, &call_id, &call.name).await?;
                    self.execute(&call.name, &call.args, state.step, context)
                        .await
                }
            };
            self.fact_result(&call_id, key, &body).await?;
            emit(
                events,
                AgentEventKind::ToolResult,
                json!({"call_id": call_id, "tool": call.name, "result": body, "status": if key == "error" { "failed" } else { "completed" }}),
            );
            state.progress.responses.push(response(call, key, body));
            if index + 1 < calls.len()
                && !crate::sim::mutated(crate::sim::Mutation::MovedForgetsResults)
            {
                self.persist(state).await?;
            }
        }
        let mut responses = std::mem::take(&mut state.progress.responses);
        for message in self.config.engine.take_messages() {
            state.step += 1;
            if let Some(last) = responses.last_mut() {
                append(last, &system_message(&message.render()));
            }
        }
        state.streak = match calls.iter().filter(|call| !passive(call)).count() {
            0 => state.streak,
            1 => state.streak + 1,
            _ => 0,
        };
        if state.streak >= 2 {
            state.streak = 0;
            state.step += 1;
            if let Some(last) = responses.last_mut() {
                append(last, &system_message(REMINDER));
            }
        }
        state.push(Content::user(responses));
        self.persist(state).await
    }

    pub(crate) async fn side_question(&self, question: String) -> Result<SideAnswer> {
        let state = self.load();
        if state.contents.is_empty() {
            return Err(NanocodeError::InvalidRequest(
                "a side question requires a completed turn".into(),
            ));
        }
        let mut parts = state
            .contents
            .last()
            .filter(|content| content.role == "model")
            .map(|content| {
                content
                    .calls()
                    .map(|call| response(call, "output", RUNNING.to_owned()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        parts.push(Part::text(
            nanocode_agent_core::backend::side_question_prompt(&question),
        ));
        let extra = Content::user(parts);
        let (runtime, _events) =
            nanocode_agent_core::backend::BackendRuntime::new(self.config.session.clone());
        let max_output_tokens = self.profile.get().max_output_tokens;
        let reply = self
            .origin(None)
            .scope(self.request(
                &state,
                Some(&extra),
                max_output_tokens,
                state.calls,
                &runtime.events(),
            ))
            .await?;
        let text = visible(&reply.parts);
        if text.trim().is_empty() {
            return Err(NanocodeError::InvalidRequest(
                match reply.parts.iter().any(|part| part.function_call.is_some()) {
                    true => {
                        "the side question response called a tool instead of answering directly"
                    }
                    false => "the side question response contained no text",
                }
                .into(),
            ));
        }
        Ok(SideAnswer::new(
            text,
            Some(reported(crate::driver::Usage {
                input: reply.usage.prompt_token_count,
                cached: reply.usage.cached_content_token_count,
                output: reply.usage.candidates_token_count + reply.usage.thoughts_token_count,
                reasoning: reply.usage.thoughts_token_count,
                total: reply.usage.total_token_count,
            })),
        ))
    }
}
