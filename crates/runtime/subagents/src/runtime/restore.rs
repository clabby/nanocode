use super::*;

pub async fn resumable_sessions(
    journal: &nanocode_agent_core::backend::ChildJournal,
    root_session_id: &str,
) -> std::io::Result<Vec<String>> {
    let store = durable::JournalStore(journal.store());
    let Some(payload) = store.load(root_session_id).await? else {
        return Ok(Vec::new());
    };
    let scope: durable::PersistedScope = serde_json::from_str(&payload)
        .map_err(|error| std::io::Error::other(format!("invalid subagent journal: {error}")))?;
    drop(payload);
    if !(durable::MIN_JOURNAL_VERSION..=durable::JOURNAL_VERSION).contains(&scope.version) {
        return Err(std::io::Error::other(format!(
            "unsupported subagent journal version {}",
            scope.version
        )));
    }
    let mut written = durable::JournalCache::default().written_as(scope.version);
    let deltas = durable::load_deltas(&store, root_session_id, scope.deltas, &mut written).await?;
    let mut records = scope.agent_records;
    if let Some(pages) = scope.agent_pages {
        records.extend(pages.agent_records(&store, root_session_id).await?);
    }
    let mut agents = scope.agents;
    agents.extend(durable::load_agents(&store, root_session_id, records).await?);
    let leaves = durable::load_leaves(
        &store,
        root_session_id,
        scope.agent_leaves,
        deltas.agents,
        &mut written,
    )
    .await?;
    agents.extend(leaves.agents);
    agents.sort_by_key(|agent| agent.descriptor.id);
    let lineages = durable::journaled_lineages(root_session_id, &agents);
    let mut sessions = Vec::new();
    for (agent, lineage) in agents.into_iter().zip(lineages) {
        let checkpoint = agent.snapshot(&lineage)?;
        let (session, resume, _) = durable::restored_session(agent, checkpoint)?;
        if resume {
            sessions.push(session.descriptor.session_id);
        }
    }
    Ok(sessions)
}

#[inline(never)]
pub(super) fn restore_missing<'a>(
    reg: &'a Arc<Registry>,
    root: &'a str,
    missing: &'a mut Vec<MissingRuntime>,
) -> RegistryFuture<'a, std::io::Result<()>> {
    Box::pin(async move {
        let root = root.to_owned();
        while let Some((
            id,
            parent_id,
            parent_session,
            snapshot,
            host_context,
            schema,
            task,
            budget,
            conversation,
        )) = missing.pop()
        {
            let snapshot = match snapshot {
                Some(snapshot) => snapshot,
                None => reg.journal_snapshot(&root, id).await?,
            };
            let parent = reg
                .session_handles
                .read(|handles| handles.get(&parent_session).cloned())
                .ok_or_else(|| {
                    std::io::Error::other("subagent parent runtime is unavailable for rehydration")
                })?;
            let contract = OutputContract::compile(&schema)?;
            let needs_assignment = !snapshot.has_conversation() && !conversation;
            let context = crate::model::AgentContext {
                model: snapshot.model(),
                thinking: snapshot.thinking(),
                reasoning_mode: snapshot.reasoning_mode(),
            };
            let (agent, events) = parent
                .restore_runtime(snapshot, host_context)
                .await
                .map_err(std::io::Error::other)?;
            let (start, ready) = oneshot::channel();
            let event_task = forward_events(
                root.clone(),
                id,
                parent_id,
                events,
                ready,
                Arc::downgrade(reg),
                reg.updates.clone(),
            );
            let (harness, task) = harness::spawn(
                root.clone(),
                id,
                Some(context.clone()),
                agent,
                reg.capacity.clone(),
                Arc::downgrade(reg),
                contract.schema,
                needs_assignment.then(|| crate::model::agent_prompt(id, &task, budget)),
            );
            let resident = harness.clone();
            let scoped = root.clone();
            let installed = reg
                .state
                .call(move |state| {
                    let session = state
                        .scopes
                        .get_mut(&scoped)
                        .expect("locked scope")
                        .sessions
                        .get_mut(&id)
                        .expect("retained child");
                    if matches!(session.status, AgentStatus::Closing | AgentStatus::Closed) {
                        return None;
                    }
                    session.descriptor.model = Some(context.model);
                    session.descriptor.thinking = Some(context.thinking);
                    session.descriptor.reasoning_mode = context.reasoning_mode;
                    session.residency = Residency::Resident(resident);
                    session.harness_task = Some(task);
                    session.event_task = Some(event_task);

                    Some((
                        std::mem::take(&mut session.announce).then(|| {
                            let mut descriptor = session.descriptor.clone();
                            descriptor.task = session.binding_task.clone();
                            descriptor
                        }),
                        session.hibernated.take(),
                        session.hibernations,
                        session.turn.as_ref().is_some_and(|turn| turn.interrupted),
                        Arc::clone(&session.marks),
                    ))
                })
                .await;
            let Some((announce, hibernated, hibernations, interrupted, marks)) = installed else {
                drop(start);
                if let Err(error) = harness.close().await {
                    nanocode_trace::debug!(%id, %error, "a restored subagent that began closing did not shut down");
                }
                return Ok(());
            };
            if let Some(descriptor) = announce {
                reg.send(&root, AgentUpdate::Added(descriptor));
            }
            let _ = start.send((hibernations, marks));
            if let Some(mut hibernated) = hibernated {
                let capacity = hibernated
                    .capacity
                    .take()
                    .unwrap_or_else(|| reg.capacity.overdraw());
                harness.resume(hibernated, capacity, interrupted).await?;
            }
        }
        Ok(())
    })
}

#[inline(never)]
pub(super) fn rehydrate<'a>(
    reg: &'a Arc<Registry>,
    session_id: &'a str,
    to: AgentId,
    purpose: MessagePurpose,
) -> RegistryFuture<'a, std::io::Result<Claimed>> {
    Box::pin(async move {
        let _warm = reg.warm(&reg.root_of(session_id), vec![to]).await?;
        let session = session_id.to_owned();
        if let Some((root, lane)) = reg
            .state
            .call(move |state| state.release_unstarted(&session, to))
            .await
        {
            reg.enqueue_status(&root, to, lane, OrderedJob::Status(AgentStatus::Pending));
            let turn = reg.capacity.acquire(Waiter::Spawn).await;
            Box::pin(crate::tools::start_unstarted(reg, &root, to, turn)).await;
        }
        let session = session_id.to_owned();
        let root = reg
            .state
            .call(move |state| -> std::io::Result<_> {
                let session_id = session.as_str();
                let root = state.root_session_id(session_id).to_owned();
                let scope = state
                    .scopes
                    .get(&root)
                    .ok_or_else(|| std::io::Error::other(format!("unknown agent_id {to}")))?;
                if scope.topology.agent_for_session(session_id) == Some(to) {
                    return Err(std::io::Error::other("agents cannot message themselves"));
                }
                if purpose == MessagePurpose::Delegate {
                    scope.topology.authorize(session_id, to)?;
                }
                scope
                    .sessions
                    .get(&to)
                    .ok_or_else(|| std::io::Error::other(format!("unknown agent_id {to}")))?;
                Ok(root)
            })
            .await?;
        let hibernates = reg.hibernates();
        let (mut missing, claimed) = loop {
            let scoped = root.clone();
            let mailbox = reg.state.clone();
            let claimed = reg
                .state
                .call(move |state| -> std::io::Result<_> {
                    let scope = state
                        .scopes
                        .get_mut(&scoped)
                        .ok_or_else(|| std::io::Error::other(format!("unknown agent_id {to}")))?;
                    let missing = Registry::missing_runtimes(scope, &scoped, to)?;
                    let parents = std::iter::successors(
                        scope
                            .sessions
                            .get(&to)
                            .and_then(|session| session.descriptor.parent),
                        |id| {
                            scope
                                .sessions
                                .get(id)
                                .and_then(|session| session.descriptor.parent)
                        },
                    )
                    .filter(|_| hibernates);
                    let mut wants = parents
                        .chain([to])
                        .map(|id| (id, ClaimKind::Pin))
                        .collect::<BTreeMap<_, _>>();
                    let restore = if crate::sim::mutated(crate::sim::Mutation::RestoreSharesClaim) {
                        ClaimKind::Pin
                    } else {
                        ClaimKind::Move
                    };
                    wants.extend(missing.iter().map(|(id, ..)| (*id, restore)));
                    Ok(match scope.claim(wants) {
                        Some(held) => Ok((
                            missing,
                            Claimed {
                                state: mailbox,
                                root: scoped,
                                held,
                            },
                        )),
                        None => Err(scope.residency.subscribe()),
                    })
                })
                .await?;
            match claimed {
                Ok(claimed) => break claimed,
                Err(mut released) => released
                    .changed()
                    .await
                    .map_err(|_| std::io::Error::other("subagent scope disappeared"))?,
            }
        };
        reg.restore_missing(&root, &mut missing).await?;
        Ok(claimed.pin(to))
    })
}

#[inline(never)]
pub(super) fn restore_inner<'a>(
    reg: &'a Arc<Registry>,
    root_session_id: &'a str,
) -> RegistryFuture<'a, std::io::Result<RestoreReport>> {
    Box::pin(async move {
        let store = reg
            .store_for(root_session_id)
            .ok_or_else(|| std::io::Error::other("no subagent store is installed"))?;
        let Some(payload) = store.load(root_session_id).await? else {
            return Ok(RestoreReport::default());
        };
        let journal: durable::PersistedScope = serde_json::from_str(&payload)
            .map_err(|error| std::io::Error::other(format!("invalid subagent journal: {error}")))?;

        drop(payload);
        if !(durable::MIN_JOURNAL_VERSION..=durable::JOURNAL_VERSION).contains(&journal.version) {
            return Err(std::io::Error::other(format!(
                "unsupported subagent journal version {}",
                journal.version
            )));
        }
        let folds_all = !journal.agents.is_empty()
            || !journal.agent_records.is_empty()
            || journal.agent_pages.is_some()
            || (journal.version <= durable::FIELDS_JOURNAL_VERSION
                && reg.journal_version.load(Ordering::Relaxed) > durable::FIELDS_JOURNAL_VERSION);
        let mut written = durable::JournalCache::default().written_as(journal.version);
        let deltas = durable::load_deltas(
            store.as_ref(),
            root_session_id,
            journal.deltas,
            &mut written,
        )
        .await?;
        let messages = journal
            .messages
            .hydrate(
                store.as_ref(),
                root_session_id,
                deltas.messages,
                &mut written,
            )
            .await?;
        let mut records = journal.agent_records;
        if let Some(pages) = journal.agent_pages {
            records.extend(pages.agent_records(store.as_ref(), root_session_id).await?);
        }
        let mut agents = journal.agents;
        agents.extend(durable::load_agents(store.as_ref(), root_session_id, records).await?);
        let leaves = durable::load_leaves(
            store.as_ref(),
            root_session_id,
            journal.agent_leaves,
            deltas.agents,
            &mut written,
        )
        .await?;
        agents.extend(leaves.agents);
        let cold = leaves.cold;
        let mut written = written.stored(folds_all);
        agents.sort_by_key(|agent| agent.descriptor.id);
        let spawning_turns = agents
            .iter()
            .filter_map(durable::PersistedAgent::unmarked_spawning_turn)
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        if !spawning_turns.is_empty() {
            let cancelled = store
                .cancelled_turns(root_session_id, spawning_turns.into_iter().collect())
                .await
                .map_err(|error| {
                    std::io::Error::new(
                        error.kind(),
                        format!("could not read the root's cancelled turns: {error}"),
                    )
                })?;
            for agent in &mut agents {
                if agent
                    .unmarked_spawning_turn()
                    .is_some_and(|turn| cancelled.iter().any(|cancelled| cancelled == turn))
                {
                    agent.cancelled_revision = Some(agent.next_instruction_revision);
                }
            }
        }

        let lazy = journal.version >= durable::LAZY_CHECKPOINT_VERSION;
        let mut stored = HashMap::new();
        for agent in &mut agents {
            if lazy {
                if let Some(key) = &agent.checkpoint_ref {
                    stored.insert(
                        agent.descriptor.id,
                        durable::JournalCheckpoint::stored(key.clone(), None),
                    );
                }
            } else if let Some(checkpoint) = agent.hydrate(store.as_ref(), root_session_id).await? {
                stored.insert(agent.descriptor.id, checkpoint);
            }
        }
        let registry = reg.shared();
        let root = root_session_id.to_owned();
        let records_sessions = store.records_sessions();
        let (report, restored_sessions, waited, written) = reg
            .state
            .call(move |state| -> std::io::Result<_> {
                let root_session_id = root.as_str();
                let mut report = RestoreReport::default();
                let mut waited = Vec::new();
                let mut checkpoints = Vec::new();
                if state
                    .scopes
                    .get(root_session_id)
                    .is_some_and(|scope| !scope.sessions.is_empty() || !scope.cold.is_empty())
                {
                    return Err(std::io::Error::other(
                        "subagent scope already has live children and cannot be restored",
                    ));
                }
                let lineages = durable::journaled_lineages(root_session_id, &agents);
                let mut restored_sessions = Vec::with_capacity(agents.len());
                let mut stubs = cold.iter().flat_map(|(_, _, stubs)| stubs).peekable();
                for (agent, lineage) in agents.into_iter().zip(lineages) {
                    let id = agent.descriptor.id;
                    while let Some(stub) = stubs.next_if(|stub| stub.id < id) {
                        state.insert_cold(root_session_id, stub)?;
                        restored_sessions.push(stub.session.clone());
                        report.restored += 1;
                    }
                    let checkpoint = agent.snapshot(&lineage)?;
                    let (session, resume, lost) = durable::restored_session(agent, checkpoint)?;
                    if session.conversation
                        && registry
                            .as_ref()
                            .is_some_and(|registry| !registry.records_sessions(root_session_id))
                    {
                        session.marks.captured.store(true, Ordering::Release);
                    }
                    if session.completion.revision().is_some()
                        && session.status.wakes_parent()
                        && !resume
                    {
                        report.completed.push(id);
                    }
                    match (stored.remove(&id), session.stored_runtime.checkpoint()) {
                        (Some(checkpoint), _) => checkpoints.push((id, checkpoint)),

                        (None, Some(snapshot)) => {
                            let checkpoint = durable::JournalCheckpoint::encode(
                                snapshot.clone(),
                                records_sessions,
                            )?;
                            if let Some(registry) = &registry {
                                registry.insert_checkpoint(root_session_id, id, checkpoint);
                            }
                        }
                        (None, None) => {}
                    }
                    restored_sessions.push(session.descriptor.session_id.clone());
                    waited.extend(remembered(id, &session));
                    if matches!(session.stored_runtime, StoredRuntime::Unstarted(_)) {
                        report.unstarted.push(id);
                    }
                    let generation = state.insert_restored(root_session_id, session, !resume)?;
                    if let Some(written) = &mut written
                        && !resume
                    {
                        written.restored(id, generation);
                    }
                    report.restored += 1;
                    if resume {
                        report.interrupted.push(id);
                    }
                    if lost {
                        report.unrecoverable.push(id);
                    }
                }
                for stub in stubs {
                    state.insert_cold(root_session_id, stub)?;
                    restored_sessions.push(stub.session.clone());
                    report.restored += 1;
                }
                let scope = state.scope_mut(root_session_id);
                scope.messages = messages;
                for (leaf, stored, stubs) in &cold {
                    if let Some(pending) = &stored.pending {
                        report.unstarted.extend(pending.unstarted.iter().copied());
                    }
                    scope.insert_cold_leaf(*leaf, stored.clone(), stubs);
                }
                report.unstarted.sort_unstable();
                if let Some(registry) = &registry {
                    registry.checkpoints.update(|stored| {
                        for (id, checkpoint) in &checkpoints {
                            stored.insert((root_session_id.to_owned(), *id), checkpoint.clone());
                        }
                    });
                    if written.is_none() {
                        registry.checkpoint_changes.update(|changes| {
                            changes
                                .entry(root_session_id.to_owned())
                                .or_default()
                                .extend(checkpoints.iter().map(|(id, _)| *id));
                        });
                    }
                }
                Ok((report, restored_sessions, waited, written))
            })
            .await?;
        if let Some(written) = written {
            reg.send_journal(
                root_session_id,
                Some(JournalRequest::Stored(Box::new(written))),
            );
        }
        reg.roots.update(|roots| {
            for session_id in &restored_sessions {
                roots.insert(session_id.clone(), root_session_id.to_owned());
            }
        });
        if !waited.is_empty() {
            reg.wait_reported
                .update(|reported| reported.extend(waited.iter().cloned()));
        }

        if !report.interrupted.is_empty() || !report.completed.is_empty() {
            reg.save_scope(root_session_id).await.map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!("could not record subagent resume attempts: {error}"),
                )
            })?;
        }
        if reg.manual_resume.load(std::sync::atomic::Ordering::SeqCst) {
            reg.deferred_resume.update(|deferred| {
                deferred.insert(
                    root_session_id.to_owned(),
                    report.interrupted.iter().copied().collect(),
                );
            });
        } else {
            reg.pending_resume.update(|pending| {
                pending.insert(
                    root_session_id.to_owned(),
                    report.interrupted.iter().copied().collect(),
                );
            });
        }
        reg.changed();
        Ok(report)
    })
}

#[inline(never)]
pub(super) fn warm<'a>(
    reg: &'a Registry,
    root_session_id: &'a str,
    ids: Vec<AgentId>,
) -> RegistryFuture<'a, std::io::Result<Option<Warm>>> {
    Box::pin(async move {
        let Some(store) = reg.store_for(root_session_id) else {
            return Ok(None);
        };
        let root = root_session_id.to_owned();
        let (leaves, cold) = reg
            .state
            .call(move |state| {
                let Some(scope) = state.scopes.get_mut(&root) else {
                    return (Vec::new(), false);
                };
                let leaves = scope.warming(&ids);
                for leaf in &leaves {
                    *scope.pins.entry(*leaf).or_default() += 1;
                }
                let cold = leaves.iter().any(|leaf| scope.cold.contains_key(leaf));
                (leaves.into_iter().collect::<Vec<_>>(), cold)
            })
            .await;
        if leaves.is_empty() {
            return Ok(None);
        }
        let warm = Warm {
            state: reg.state.clone(),
            registry: reg.this.get().cloned().unwrap_or_default(),
            root: root_session_id.to_owned(),
            leaves,
        };
        if cold {
            reg.thaw(root_session_id, store.as_ref(), &warm.leaves)
                .instrument(nanocode_trace::info_span!(
                    "subagent.warm",
                    session.root_id = root_session_id,
                    leaf.count = warm.leaves.len()
                ))
                .await?;
        }
        Ok(Some(warm))
    })
}

#[inline(never)]
pub(super) fn thaw<'a>(
    reg: &'a Registry,
    root_session_id: &'a str,
    store: &'a dyn SubagentStore,
    leaves: &'a [u64],
) -> RegistryFuture<'a, std::io::Result<()>> {
    Box::pin(async move {
        let locks = reg.scope_locks(root_session_id);
        loop {
            let mut thawed = locks.thawed.subscribe();
            let (cold, others) = claim_cold(reg, root_session_id, leaves).await;
            if !cold.is_empty() {
                let claim = Thawing {
                    state: reg.state.clone(),
                    root: root_session_id.to_owned(),
                    leaves: cold.iter().map(|(leaf, _)| *leaf).collect(),
                    thawed: locks.thawed.clone(),
                };
                thaw_claimed(reg, root_session_id, store, cold).await?;
                drop(claim);
            } else if others {
                thawed
                    .changed()
                    .await
                    .expect("the scope's locks keep its thaw sender");
            }
            if !others {
                return Ok(());
            }
        }
    })
}

async fn claim_cold(
    reg: &Registry,
    root_session_id: &str,
    leaves: &[u64],
) -> (Vec<(u64, Arc<str>)>, bool) {
    let root = root_session_id.to_owned();
    let wanted = leaves.to_vec();
    reg.state
        .call(move |state| {
            let Some(scope) = state.scopes.get_mut(&root) else {
                return (Vec::new(), false);
            };
            let mut others = false;
            let mut cold = Vec::new();
            for leaf in wanted {
                let Some(key) = scope.cold.get(&leaf).map(|cold| Arc::clone(&cold.key)) else {
                    continue;
                };
                if scope.thawing.insert(leaf) {
                    cold.push((leaf, key));
                } else {
                    others = true;
                }
            }
            (cold, others)
        })
        .await
}

fn thaw_claimed<'a>(
    reg: &'a Registry,
    root_session_id: &'a str,
    store: &'a dyn SubagentStore,
    cold: Vec<(u64, Arc<str>)>,
) -> RegistryFuture<'a, std::io::Result<()>> {
    Box::pin(async move {
        let thawed = cold.iter().map(|(leaf, _)| *leaf).collect::<Vec<_>>();
        let agents = durable::load_cold(store, root_session_id, cold).await?;
        let mut sessions = Vec::with_capacity(agents.len());
        let mut checkpoints = Vec::new();
        for (agent, cached) in agents {
            let id = agent.descriptor.id;
            if let Some(key) = &agent.checkpoint_ref {
                checkpoints.push((id, durable::JournalCheckpoint::stored(key.clone(), None)));
            }
            let (session, _, _) = durable::restored_session(agent, None)?;
            if session.conversation && !store.records_sessions() {
                session.marks.captured.store(true, Ordering::Release);
            }
            sessions.push((id, session, cached));
        }
        let root = root_session_id.to_owned();
        let warmed = reg
            .state
            .call(move |state| {
                let last_used = state.next_access();
                let Some(scope) = state.scopes.get_mut(&root) else {
                    return Vec::new();
                };
                for leaf in &thawed {
                    scope.remove_cold_leaf(*leaf);
                }
                sessions
                    .into_iter()
                    .map(|(id, mut session, cached)| {
                        session.last_used = last_used;
                        (id, cached.at(scope.sessions.insert_stored(id, session)))
                    })
                    .collect::<Vec<_>>()
            })
            .await;
        if !checkpoints.is_empty() {
            reg.checkpoints.update(|stored| {
                for (id, checkpoint) in &checkpoints {
                    stored.insert((root_session_id.to_owned(), *id), checkpoint.clone());
                }
            });
        }
        reg.send_journal(root_session_id, Some(JournalRequest::Warmed(warmed)));
        Ok(())
    })
}

#[inline(never)]
pub(super) fn await_restored<'a>(
    reg: &'a Registry,
    session_id: &'a str,
) -> RegistryFuture<'a, std::io::Result<()>> {
    Box::pin(async move {
        let root = reg.root_of(session_id);
        let gate = reg
            .claims
            .read(|claims| claims.restored.get(&root).cloned());
        if let Some(mut gate) = gate {
            let outcome = gate
                .wait_for(|outcome| {
                    matches!(
                        outcome,
                        RestorationState::Ready | RestorationState::Failed(_)
                    )
                })
                .await
                .map_err(|_| {
                    std::io::Error::other("subagent journal restoration ended without an outcome")
                })?
                .clone();
            if let RestorationState::Failed(error) = outcome {
                return Err(std::io::Error::other(error));
            }
        }
        Ok(())
    })
}

fn remembered(id: AgentId, session: &ChildSession) -> Vec<((String, AgentId), WaitReport)> {
    if !crate::sim::mutated(crate::sim::Mutation::RestoreRemembersWaits) {
        return Vec::new();
    }
    session
        .waited
        .iter()
        .map(|waited| {
            (
                (waited.session.clone(), id),
                WaitReport::Wait(waited.mark, None),
            )
        })
        .collect()
}
