use std::collections::BTreeMap;

use crate::{EncodedPayload, Error, Result};

const STATE_FORMAT: u8 = 4;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Transition {
    ExecutionAdvanced {
        operation_id: String,

        continuation: EncodedPayload,
    },

    OperationAccepted {
        operation_id: String,

        input: EncodedPayload,
    },

    StepStarted {
        operation_id: String,

        step_id: String,

        kind: String,

        input: EncodedPayload,
    },

    StepCompleted {
        operation_id: String,

        step_id: String,

        output: EncodedPayload,
    },

    SteerAccepted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,

        operation_id: String,

        steer_index: u32,

        accepted_after_model_call_index: u32,

        input: EncodedPayload,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        input_key: Option<String>,
    },

    SteerWithdrawn {
        operation_id: String,

        steer_index: u32,
    },

    SteerBound {
        operation_id: String,

        steer_index: u32,

        model_call_index: u32,
    },

    OperationCompleted {
        operation_id: String,

        checkpoint: EncodedPayload,

        output: EncodedPayload,
    },

    OperationFailed {
        operation_id: String,

        checkpoint: EncodedPayload,

        error: String,
    },

    OperationCancelled {
        operation_id: String,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        checkpoint: Option<EncodedPayload>,
    },

    CheckpointCommitted {
        checkpoint: EncodedPayload,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationStatus {
    Pending,

    Completed {
        checkpoint: EncodedPayload,

        output: EncodedPayload,
    },

    Failed {
        checkpoint: EncodedPayload,

        error: String,
    },

    Cancelled {
        checkpoint: Option<EncodedPayload>,
    },
}

impl OperationStatus {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    EffectPending,

    Completed(EncodedPayload),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StepState {
    pub kind: String,

    pub input: EncodedPayload,

    pub status: StepStatus,

    pub attempts: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SteerState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    pub input: EncodedPayload,

    pub accepted_after_model_call_index: u32,

    pub model_call_index: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct IdentifiedSteerReceipt {
    pub input_key: String,

    pub index: u32,

    pub withdrawn: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationState {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub steer_receipts: BTreeMap<String, IdentifiedSteerReceipt>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<EncodedPayload>,

    #[serde(default)]
    pub retired_model_calls: u32,

    pub retired_steers: u32,

    pub input: EncodedPayload,

    pub status: OperationStatus,

    pub steps: BTreeMap<String, StepState>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steers: Vec<SteerState>,
    accepted_order: u64,
}

impl OperationState {
    pub const fn accepted_order(&self) -> u64 {
        self.accepted_order
    }

    pub fn cancellation_requires_checkpoint(&self) -> bool {
        self.continuation.is_some()
            || self.retired_model_calls != 0
            || !self.steps.is_empty()
            || !self.steers.is_empty()
    }

    fn retire_steps(&mut self) {
        for (id, step) in &self.steps {
            if step.kind == "model_call"
                && matches!(step.status, StepStatus::Completed(_))
                && let Some(index) = id
                    .strip_prefix("model-")
                    .and_then(|id| id.parse::<u32>().ok())
            {
                self.retired_model_calls = self.retired_model_calls.max(index);
            }
        }
        self.steps.clear();
        let consumed = self
            .steers
            .iter()
            .take_while(|steer| {
                steer
                    .model_call_index
                    .is_some_and(|index| index <= self.retired_model_calls)
            })
            .count();

        self.retired_steers += consumed as u32;
        self.steers.drain(..consumed);
    }
}

#[derive(Clone, Debug, Default)]
pub struct JournalState {
    revision: u64,
    operations: BTreeMap<String, OperationState>,
    latest_checkpoint: Option<(u64, EncodedPayload)>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableCheckpoint {
    format: u8,
    operations: BTreeMap<String, OperationState>,
    latest_checkpoint: Option<EncodedPayload>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "R: serde::Deserialize<'de>"))]
pub struct RetainedCheckpoint<R> {
    pub nanocode_durable_state: DurableCheckpoint,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nanocode_session: Option<R>,
}

#[derive(serde::Serialize)]
struct DurableCheckpointRef<'a> {
    format: u8,
    operations: &'a BTreeMap<String, OperationState>,
    latest_checkpoint: Option<&'a EncodedPayload>,
}

#[derive(serde::Serialize)]
struct RetainedCheckpointRef<'a, R> {
    nanocode_durable_state: DurableCheckpointRef<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nanocode_session: Option<&'a R>,
}

impl JournalState {
    pub fn stage_records(&mut self) -> Vec<crate::StoreRecord> {
        self.stage_records_unless(&|_| false)
    }

    pub fn stage_records_unless(
        &mut self,
        known: &dyn Fn(&str) -> bool,
    ) -> Vec<crate::StoreRecord> {
        let mut records = Vec::new();
        for operation in self.operations.values_mut() {
            operation.input.stage_unless(&mut records, known);
            if let Some(value) = &mut operation.continuation {
                value.stage_unless(&mut records, known);
            }
            match &mut operation.status {
                OperationStatus::Completed { checkpoint, output } => {
                    checkpoint.stage_unless(&mut records, known);
                    output.stage_unless(&mut records, known);
                }
                OperationStatus::Failed { checkpoint, .. } => {
                    checkpoint.stage_unless(&mut records, known);
                }
                OperationStatus::Cancelled {
                    checkpoint: Some(value),
                } => value.stage_unless(&mut records, known),
                _ => {}
            }
            for step in operation.steps.values_mut() {
                step.input.stage_unless(&mut records, known);
                if let StepStatus::Completed(output) = &mut step.status {
                    output.stage_unless(&mut records, known);
                }
            }
            for steer in &mut operation.steers {
                steer.input.stage_unless(&mut records, known);
            }
        }
        if let Some((_, value)) = &mut self.latest_checkpoint {
            value.stage_unless(&mut records, known);
        }
        records.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        records.dedup_by(|a, b| a.key == b.key);
        records
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn operations(&self) -> &BTreeMap<String, OperationState> {
        &self.operations
    }

    #[must_use]
    pub fn operation(&self, operation_id: &str) -> Option<&OperationState> {
        self.operations.get(operation_id)
    }

    #[must_use]
    pub fn pending_operations(&self) -> Vec<(&str, &OperationState)> {
        let mut operations = self
            .operations
            .iter()
            .filter(|(_, operation)| !operation.status.is_terminal())
            .map(|(id, operation)| (id.as_str(), operation))
            .collect::<Vec<_>>();
        operations.sort_by_key(|(_, operation)| operation.accepted_order);
        operations
    }

    pub fn first_pending_operation(&self) -> Option<(&str, &OperationState)> {
        self.first_pending_operation_where(|_| true)
    }

    pub fn first_pending_operation_where(
        &self,
        mut predicate: impl FnMut(&str) -> bool,
    ) -> Option<(&str, &OperationState)> {
        self.operations
            .iter()
            .filter(|(id, operation)| !operation.status.is_terminal() && predicate(id.as_str()))
            .min_by_key(|(_, operation)| operation.accepted_order)
            .map(|(id, operation)| (id.as_str(), operation))
    }

    #[must_use]
    pub fn latest_checkpoint(&self) -> Option<&EncodedPayload> {
        self.latest_checkpoint
            .as_ref()
            .map(|(_, checkpoint)| checkpoint)
    }

    pub fn checkpoint_payload<R: serde::Serialize>(&self, session: Option<&R>) -> Result<String> {
        serde_json::to_string(&RetainedCheckpointRef {
            nanocode_durable_state: DurableCheckpointRef {
                format: STATE_FORMAT,
                operations: &self.operations,
                latest_checkpoint: self.latest_checkpoint(),
            },
            nanocode_session: session,
        })
        .map_err(Error::InvalidPayload)
    }

    pub fn retain_terminal_receipts(&mut self, limit: usize) -> bool {
        let before = self.operations.len();
        Self::retain_terminal_operations(&mut self.operations, limit);
        let mut changed = self.operations.len() != before;
        for operation in self
            .operations
            .values_mut()
            .filter(|operation| operation.status.is_terminal())
        {
            changed |= !operation.steps.is_empty() || !operation.steers.is_empty();
            operation.steps.clear();
            changed |= operation.continuation.take().is_some();
            operation.steers.clear();
        }
        changed
    }

    fn retain_terminal_operations(operations: &mut BTreeMap<String, OperationState>, limit: usize) {
        let mut terminal_orders = operations
            .values()
            .filter(|operation| operation.status.is_terminal())
            .map(|operation| operation.accepted_order)
            .collect::<Vec<_>>();
        terminal_orders.sort_unstable_by(|left, right| right.cmp(left));
        terminal_orders.truncate(limit);
        let retained = terminal_orders
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        operations.retain(|_, operation| {
            !operation.status.is_terminal() || retained.contains(&operation.accepted_order)
        });
    }

    pub fn from_checkpoint(revision: u64, checkpoint: DurableCheckpoint) -> Result<Self> {
        if revision == 0 {
            return Err(Error::InvalidState(
                "a compacted state checkpoint must have a positive revision".to_owned(),
            ));
        }
        if checkpoint.format != STATE_FORMAT {
            return Err(Error::InvalidState(format!(
                "unsupported state format {}",
                checkpoint.format
            )));
        }
        let mut accepted_orders = std::collections::BTreeSet::new();
        for (operation_id, operation) in &checkpoint.operations {
            ensure_nonempty(operation_id, "operation ID")?;
            if operation.accepted_order == 0
                || operation.accepted_order > revision
                || !accepted_orders.insert(operation.accepted_order)
            {
                return Err(Error::InvalidState(format!(
                    "operation `{operation_id}` has an invalid compacted acceptance order"
                )));
            }
            if operation.status.is_terminal() && operation.continuation.is_some() {
                return Err(Error::InvalidState(
                    "terminal operation retained active execution state".into(),
                ));
            }
            if !operation.status.is_terminal()
                && operation.retired_model_calls != 0
                && operation.continuation.is_none()
            {
                return Err(Error::InvalidState(
                    "retired model batches have no current conversation".into(),
                ));
            }
            for (step_id, step) in &operation.steps {
                ensure_nonempty(step_id, "step ID")?;
                ensure_nonempty(&step.kind, "step kind")?;
                if step.attempts == 0 {
                    return Err(Error::InvalidState(format!(
                        "step `{step_id}` in operation `{operation_id}` has no committed start"
                    )));
                }
            }
            let mut previous_model_call_index = None;
            let mut saw_unbound_steer = false;
            for (offset, steer) in operation.steers.iter().enumerate() {
                if steer.accepted_after_model_call_index == 0
                    || steer.model_call_index.is_some_and(|model_call_index| {
                        model_call_index <= steer.accepted_after_model_call_index
                    })
                {
                    return Err(Error::InvalidState(format!(
                        "steer {} in operation `{operation_id}` has an invalid model boundary",
                        offset + 1 + operation.retired_steers as usize
                    )));
                }
                match steer.model_call_index {
                    Some(current) => {
                        if saw_unbound_steer {
                            return Err(Error::InvalidState(format!(
                                "steer {} in operation `{operation_id}` was bound after an unbound steer",
                                offset + 1 + operation.retired_steers as usize
                            )));
                        }
                        if previous_model_call_index.is_some_and(|previous| current < previous) {
                            return Err(Error::InvalidState(format!(
                                "steer {} in operation `{operation_id}` moved before an earlier steer",
                                offset + 1 + operation.retired_steers as usize
                            )));
                        }
                        previous_model_call_index = Some(current);
                    }
                    None => saw_unbound_steer = true,
                }
            }
            if matches!(operation.status, OperationStatus::Completed { .. }) {
                ensure_completed_steers_consumed(operation_id, operation)?;
            }
            if matches!(
                &operation.status,
                OperationStatus::Cancelled { checkpoint: None }
            ) && operation.cancellation_requires_checkpoint()
            {
                return Err(Error::InvalidState(format!(
                    "started operation `{operation_id}` was cancelled without a checkpoint"
                )));
            }
        }

        let latest_checkpoint = checkpoint.latest_checkpoint.map(|latest| {
            let shared = checkpoint.operations.values().rev().find_map(|operation| {
                let candidate = match &operation.status {
                    OperationStatus::Completed { checkpoint, .. }
                    | OperationStatus::Failed { checkpoint, .. }
                    | OperationStatus::Cancelled {
                        checkpoint: Some(checkpoint),
                    } => checkpoint,
                    _ => return None,
                };
                (candidate == &latest).then(|| candidate.clone())
            });
            (revision, shared.unwrap_or(latest))
        });
        let state = Self {
            revision,
            operations: checkpoint.operations,
            latest_checkpoint,
        };
        for (operation_id, operation) in &state.operations {
            if matches!(
                &operation.status,
                OperationStatus::Completed { .. }
                    | OperationStatus::Failed { .. }
                    | OperationStatus::Cancelled {
                        checkpoint: Some(_)
                    }
            ) {
                state.ensure_prior_operations_terminal(operation_id)?;
            }
        }
        Ok(state)
    }

    pub fn validate_transition(&self, revision: u64, entry: &Transition) -> Result<()> {
        let expected_revision = self.revision.checked_add(1).ok_or_else(|| {
            Error::InvalidState("state revision exceeded the u64 range".to_owned())
        })?;
        if revision != expected_revision {
            return Err(Error::InvalidState(format!(
                "expected revision {}, found {revision}",
                expected_revision
            )));
        }
        self.validate(entry)
    }

    pub fn apply_transition(&mut self, revision: u64, entry: Transition) -> Result<()> {
        self.validate_transition(revision, &entry)?;
        self.apply(revision, entry)?;
        self.revision = revision;
        Ok(())
    }

    pub fn apply_within(&mut self, revision: u64, entry: Transition) -> Result<()> {
        if revision == 0 || revision != self.revision {
            return Err(Error::InvalidState(format!(
                "expected revision {}, found {revision}",
                self.revision
            )));
        }
        if matches!(entry, Transition::OperationAccepted { .. })
            && self
                .operations
                .values()
                .any(|operation| operation.accepted_order == revision)
        {
            return Err(Error::InvalidState(
                "one revision accepted more than one operation".to_owned(),
            ));
        }
        self.validate(&entry)?;
        self.apply(revision, entry)
    }

    pub fn retracted(revision: u64) -> Self {
        Self {
            revision,
            ..Self::default()
        }
    }

    pub fn advance_revision(&mut self, revision: u64) -> Result<()> {
        let expected_revision = self.revision.checked_add(1).ok_or_else(|| {
            Error::InvalidState("state revision exceeded the u64 range".to_owned())
        })?;
        if revision != expected_revision {
            return Err(Error::InvalidState(format!(
                "expected revision {}, found {revision}",
                expected_revision
            )));
        }
        self.revision = revision;
        Ok(())
    }

    fn validate(&self, entry: &Transition) -> Result<()> {
        if let Some(operation_id) = entry.operation_id() {
            ensure_nonempty(operation_id, "operation ID")?;
        }
        match entry {
            Transition::ExecutionAdvanced { operation_id, .. } => {
                self.ensure_prior_operations_terminal(operation_id)?;
                let operation = self.pending_operation(operation_id)?;
                if operation
                    .steps
                    .values()
                    .any(|step| matches!(step.status, StepStatus::EffectPending))
                {
                    return Err(Error::InvalidState(format!(
                        "operation `{operation_id}` cannot advance past an unsettled effect"
                    )));
                }
            }
            Transition::OperationAccepted { operation_id, .. } => {
                if self.operations.contains_key(operation_id) {
                    return Err(Error::InvalidState(format!(
                        "operation `{operation_id}` was accepted more than once"
                    )));
                }
            }
            Transition::StepStarted {
                operation_id,
                step_id,
                kind,
                input,
            } => {
                ensure_nonempty(step_id, "step ID")?;
                ensure_nonempty(kind, "step kind")?;
                self.ensure_prior_operations_terminal(operation_id)?;
                let operation = self.pending_operation(operation_id)?;
                if kind == "model_call"
                    && step_id
                        .strip_prefix("model-")
                        .and_then(|id| id.parse::<u32>().ok())
                        .is_some_and(|index| index <= operation.retired_model_calls)
                {
                    return Err(Error::InvalidState(
                        "cannot execute a retired model batch".into(),
                    ));
                }
                if let Some(step) = operation.steps.get(step_id) {
                    if step.kind != *kind || step.input != *input {
                        return Err(Error::InvalidState(format!(
                            "step `{step_id}` in operation `{operation_id}` changed definition"
                        )));
                    }
                    if matches!(step.status, StepStatus::Completed(_)) {
                        return Err(Error::InvalidState(format!(
                            "settled step `{step_id}` in operation `{operation_id}` restarted"
                        )));
                    }
                    if step.attempts == u32::MAX {
                        return Err(Error::InvalidState(format!(
                            "step `{step_id}` in operation `{operation_id}` exceeded the attempt counter range"
                        )));
                    }
                }
            }
            Transition::StepCompleted {
                operation_id,
                step_id,
                output: _,
            } => {
                ensure_nonempty(step_id, "step ID")?;
                self.ensure_prior_operations_terminal(operation_id)?;
                let operation = self.pending_operation(operation_id)?;
                let step = operation.steps.get(step_id).ok_or_else(|| {
                    Error::InvalidState(format!(
                        "step `{step_id}` in operation `{operation_id}` completed before start"
                    ))
                })?;
                match &step.status {
                    StepStatus::EffectPending => {}
                    StepStatus::Completed(_) => {
                        return Err(Error::InvalidState(format!(
                            "step `{step_id}` in operation `{operation_id}` completed more than once"
                        )));
                    }
                }
            }
            Transition::SteerAccepted {
                operation_id,
                steer_index,
                accepted_after_model_call_index,
                input: _,
                input_key: _,
                message_id,
            } => {
                self.ensure_prior_operations_terminal(operation_id)?;
                if *accepted_after_model_call_index == 0 {
                    return Err(Error::InvalidState(format!(
                        "steer {steer_index} in operation `{operation_id}` has an invalid acceptance boundary"
                    )));
                }
                let operation = self.pending_operation(operation_id)?;
                if message_id
                    .as_ref()
                    .is_some_and(|id| id.is_empty() || operation.steer_receipts.contains_key(id))
                {
                    return Err(Error::InvalidState(
                        "steer identity was already accepted or is empty".into(),
                    ));
                }
                let expected = u32::try_from(operation.steers.len())
                    .ok()
                    .and_then(|length| length.checked_add(operation.retired_steers)?.checked_add(1))
                    .ok_or_else(|| {
                        Error::InvalidState(format!(
                            "operation `{operation_id}` exceeded the steer counter range"
                        ))
                    })?;
                if *steer_index != expected {
                    return Err(Error::InvalidState(format!(
                        "operation `{operation_id}` expected steer {expected}, found {steer_index}"
                    )));
                }
            }
            Transition::SteerWithdrawn {
                operation_id,
                steer_index,
            } => {
                self.ensure_prior_operations_terminal(operation_id)?;
                let operation = self.pending_operation(operation_id)?;
                if steer_index
                    .checked_sub(operation.retired_steers)
                    .and_then(|index| usize::try_from(index).ok())
                    != Some(operation.steers.len())
                    || !operation
                        .steers
                        .last()
                        .is_some_and(|steer| steer.model_call_index.is_none())
                {
                    return Err(Error::InvalidState(format!(
                        "steer {steer_index} in operation `{operation_id}` is not the latest unbound steer"
                    )));
                }
            }
            Transition::SteerBound {
                operation_id,
                steer_index,
                model_call_index,
            } => {
                self.ensure_prior_operations_terminal(operation_id)?;
                if *model_call_index == 0 {
                    return Err(Error::InvalidState(format!(
                        "steer {steer_index} in operation `{operation_id}` has an invalid model boundary"
                    )));
                }
                let operation = self.pending_operation(operation_id)?;
                let steer = steer_index
                    .checked_sub(operation.retired_steers)
                    .and_then(|index| index.checked_sub(1))
                    .and_then(|index| usize::try_from(index).ok())
                    .and_then(|index| operation.steers.get(index))
                    .ok_or_else(|| {
                        Error::InvalidState(format!(
                            "steer {steer_index} in operation `{operation_id}` was bound before acceptance"
                        ))
                    })?;
                if *model_call_index <= steer.accepted_after_model_call_index {
                    return Err(Error::InvalidState(format!(
                        "steer {steer_index} in operation `{operation_id}` cannot bind to model call {model_call_index} after acceptance at {}",
                        steer.accepted_after_model_call_index
                    )));
                }
                if steer.model_call_index.is_some() {
                    return Err(Error::InvalidState(format!(
                        "steer {steer_index} in operation `{operation_id}` was bound more than once"
                    )));
                }
                if *steer_index - operation.retired_steers > 1 {
                    let previous_index = usize::try_from(
                        *steer_index - operation.retired_steers - 2,
                    )
                    .map_err(|_| {
                        Error::InvalidState(format!(
                            "steer {steer_index} in operation `{operation_id}` has an invalid index"
                        ))
                    })?;
                    let previous = &operation.steers[previous_index];
                    let Some(previous_model_call_index) = previous.model_call_index else {
                        return Err(Error::InvalidState(format!(
                            "steer {steer_index} in operation `{operation_id}` was bound before an earlier steer"
                        )));
                    };
                    if *model_call_index < previous_model_call_index {
                        return Err(Error::InvalidState(format!(
                            "steer {steer_index} in operation `{operation_id}` moved before an earlier steer"
                        )));
                    }
                }
            }
            Transition::OperationCompleted { operation_id, .. } => {
                self.ensure_prior_operations_terminal(operation_id)?;
                let operation = self.pending_operation(operation_id)?;
                if operation
                    .steps
                    .values()
                    .any(|step| !matches!(step.status, StepStatus::Completed(_)))
                {
                    return Err(Error::InvalidState(format!(
                        "operation `{operation_id}` completed with an unfinished step"
                    )));
                }
                ensure_completed_steers_consumed(operation_id, operation)?;
            }
            Transition::OperationFailed { operation_id, .. } => {
                self.ensure_prior_operations_terminal(operation_id)?;
                self.pending_operation(operation_id)?;
            }
            Transition::OperationCancelled {
                operation_id,
                checkpoint,
            } => {
                let operation = self.pending_operation(operation_id)?;
                if checkpoint.is_some() {
                    self.ensure_prior_operations_terminal(operation_id)?;
                } else if operation.cancellation_requires_checkpoint() {
                    return Err(Error::InvalidState(format!(
                        "started operation `{operation_id}` was cancelled without a checkpoint"
                    )));
                }
            }
            Transition::CheckpointCommitted { .. } => {
                if let Some((pending_id, _)) = self.first_pending_operation() {
                    return Err(Error::InvalidState(format!(
                        "standalone checkpoint effect crossed pending operation `{pending_id}`"
                    )));
                }
            }
        }
        Ok(())
    }

    fn apply(&mut self, revision: u64, entry: Transition) -> Result<()> {
        match entry {
            Transition::ExecutionAdvanced {
                operation_id,
                continuation,
            } => {
                let operation = self.pending_operation_mut(&operation_id)?;
                operation.continuation = Some(continuation);
                operation.retire_steps();
            }
            Transition::OperationAccepted {
                operation_id,
                input,
            } => {
                self.operations.insert(
                    operation_id,
                    OperationState {
                        steer_receipts: BTreeMap::new(),
                        continuation: None,
                        retired_model_calls: 0,
                        retired_steers: 0,
                        input,
                        status: OperationStatus::Pending,
                        steps: BTreeMap::new(),
                        steers: Vec::new(),
                        accepted_order: revision,
                    },
                );
            }
            Transition::StepStarted {
                operation_id,
                step_id,
                kind,
                input,
            } => {
                let operation = self.pending_operation_mut(&operation_id)?;
                if let Some(step) = operation.steps.get_mut(&step_id) {
                    step.attempts = step.attempts.checked_add(1).ok_or_else(|| {
                        Error::InvalidState(format!(
                            "step `{step_id}` in operation `{operation_id}` exceeded the attempt counter range"
                        ))
                    })?;
                } else {
                    operation.steps.insert(
                        step_id,
                        StepState {
                            kind,
                            input,
                            status: StepStatus::EffectPending,
                            attempts: 1,
                        },
                    );
                }
            }
            Transition::StepCompleted {
                operation_id,
                step_id,
                output,
            } => {
                let operation = self.pending_operation_mut(&operation_id)?;
                let step = operation.steps.get_mut(&step_id).ok_or_else(|| {
                    Error::InvalidState(format!(
                        "step `{step_id}` in operation `{operation_id}` completed before start"
                    ))
                })?;
                step.status = StepStatus::Completed(output);
            }
            Transition::SteerAccepted {
                operation_id,
                steer_index,
                accepted_after_model_call_index,
                input,
                input_key,
                message_id,
            } => {
                let operation = self.pending_operation_mut(&operation_id)?;
                if let Some(id) = &message_id {
                    operation.steer_receipts.insert(
                        id.clone(),
                        IdentifiedSteerReceipt {
                            input_key: input_key.unwrap_or_else(|| input.key().to_owned()),
                            index: steer_index,
                            withdrawn: false,
                        },
                    );
                }
                operation.steers.push(SteerState {
                    message_id,
                    input,
                    accepted_after_model_call_index,
                    model_call_index: None,
                });
            }
            Transition::SteerBound {
                operation_id,
                steer_index,
                model_call_index,
            } => {
                let operation = self.pending_operation_mut(&operation_id)?;
                let index =
                    usize::try_from(steer_index - operation.retired_steers - 1).map_err(|_| {
                        Error::InvalidState(format!(
                            "steer {steer_index} in operation `{operation_id}` has an invalid index"
                        ))
                    })?;
                operation.steers[index].model_call_index = Some(model_call_index);
            }
            Transition::SteerWithdrawn { operation_id, .. } => {
                let operation = self.pending_operation_mut(&operation_id)?;
                if let Some(steer) = operation.steers.pop()
                    && let Some(id) = steer.message_id
                    && let Some(receipt) = operation.steer_receipts.get_mut(&id)
                {
                    receipt.withdrawn = true;
                }
            }
            Transition::OperationCompleted {
                operation_id,
                checkpoint,
                output,
            } => {
                let operation = self.pending_operation_mut(&operation_id)?;
                if operation.continuation.take().is_some() {
                    operation.retire_steps();
                }
                operation.status = OperationStatus::Completed {
                    checkpoint: checkpoint.clone(),
                    output,
                };
                self.latest_checkpoint = Some((revision, checkpoint));
            }
            Transition::OperationFailed {
                operation_id,
                checkpoint,
                error,
            } => {
                let operation = self.pending_operation_mut(&operation_id)?;
                if operation.continuation.take().is_some() {
                    operation.retire_steps();
                }
                operation.status = OperationStatus::Failed {
                    checkpoint: checkpoint.clone(),
                    error,
                };
                self.latest_checkpoint = Some((revision, checkpoint));
            }
            Transition::OperationCancelled {
                operation_id,
                checkpoint,
            } => {
                let operation = self.pending_operation_mut(&operation_id)?;
                if operation.continuation.take().is_some() {
                    operation.retire_steps();
                }
                operation.status = OperationStatus::Cancelled {
                    checkpoint: checkpoint.clone(),
                };
                if let Some(checkpoint) = checkpoint {
                    self.latest_checkpoint = Some((revision, checkpoint));
                }
            }
            Transition::CheckpointCommitted { checkpoint } => {
                self.latest_checkpoint = Some((revision, checkpoint));
            }
        }
        Ok(())
    }

    fn pending_operation_mut(&mut self, operation_id: &str) -> Result<&mut OperationState> {
        let operation = self.operations.get_mut(operation_id).ok_or_else(|| {
            Error::InvalidState(format!("operation `{operation_id}` was not accepted"))
        })?;
        if operation.status.is_terminal() {
            return Err(Error::InvalidState(format!(
                "terminal operation `{operation_id}` was changed"
            )));
        }
        Ok(operation)
    }

    fn pending_operation(&self, operation_id: &str) -> Result<&OperationState> {
        let operation = self.operations.get(operation_id).ok_or_else(|| {
            Error::InvalidState(format!("operation `{operation_id}` was not accepted"))
        })?;
        if operation.status.is_terminal() {
            return Err(Error::InvalidState(format!(
                "terminal operation `{operation_id}` was changed"
            )));
        }
        Ok(operation)
    }

    fn ensure_prior_operations_terminal(&self, operation_id: &str) -> Result<()> {
        let operation = self.operations.get(operation_id).ok_or_else(|| {
            Error::InvalidState(format!("operation `{operation_id}` was not accepted"))
        })?;
        if let Some((pending_id, _)) = self.operations.iter().find(|(id, candidate)| {
            candidate.accepted_order < operation.accepted_order
                && !candidate.status.is_terminal()
                && id.as_str() != operation_id
        }) {
            return Err(Error::InvalidState(format!(
                "operation `{operation_id}` completed before `{pending_id}`"
            )));
        }
        Ok(())
    }
}

fn ensure_completed_steers_consumed(operation_id: &str, operation: &OperationState) -> Result<()> {
    for (offset, steer) in operation.steers.iter().enumerate() {
        let steer_index = offset + 1 + operation.retired_steers as usize;
        let model_call_index = steer.model_call_index.ok_or_else(|| {
            Error::InvalidState(format!(
                "operation `{operation_id}` completed with unbound steer {steer_index}"
            ))
        })?;
        let step_id = format!("model-{model_call_index}");
        let consumed = model_call_index <= operation.retired_model_calls
            || operation.steps.get(&step_id).is_some_and(|step| {
                step.kind == "model_call" && matches!(step.status, StepStatus::Completed(_))
            });
        if !consumed {
            return Err(Error::InvalidState(format!(
                "operation `{operation_id}` completed before steer {steer_index} was consumed by `{step_id}`"
            )));
        }
    }
    Ok(())
}

impl Transition {
    fn operation_id(&self) -> Option<&str> {
        match self {
            Self::ExecutionAdvanced { operation_id, .. }
            | Self::OperationAccepted { operation_id, .. }
            | Self::StepStarted { operation_id, .. }
            | Self::StepCompleted { operation_id, .. }
            | Self::SteerAccepted { operation_id, .. }
            | Self::SteerBound { operation_id, .. }
            | Self::SteerWithdrawn { operation_id, .. }
            | Self::OperationCompleted { operation_id, .. }
            | Self::OperationFailed { operation_id, .. }
            | Self::OperationCancelled { operation_id, .. } => Some(operation_id),
            Self::CheckpointCommitted { .. } => None,
        }
    }
}

fn ensure_nonempty(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::InvalidState(format!("{name} must not be empty")));
    }
    Ok(())
}
