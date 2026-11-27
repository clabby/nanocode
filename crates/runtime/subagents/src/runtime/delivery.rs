use super::*;

#[inline(never)]
#[allow(clippy::too_many_arguments)]
pub(super) fn deliver_message<'a>(
    reg: &'a Arc<Registry>,
    session_id: &'a str,
    call_id: Option<&'a str>,
    to: AgentId,
    priority: MessagePriority,
    purpose: MessagePurpose,
    in_reply_to: Option<MessageId>,
    body: String,
    user: bool,
) -> RegistryFuture<'a, std::io::Result<MessageReceipt>> {
    Box::pin(async move {
        let target = reg.rehydrate(session_id, to, purpose).await?;
        let session = session_id.to_owned();
        let call = call_id.map(str::to_owned);
        let (prepared, delivery) = reg
            .when_messages_flow(session_id, move |state| -> std::io::Result<_> {
                let prepared = state.prepare_message(
                    &session,
                    to,
                    priority,
                    purpose,
                    in_reply_to,
                    body,
                    user,
                )?;
                let delivery = prepared
                    .harness
                    .enqueue_delivery(prepared.message.clone())?;
                state.commit_message(&prepared.root_session_id, prepared.message.clone())?;
                if let Some(call) = &call {
                    state.record_sent(
                        &prepared.root_session_id,
                        &session,
                        call,
                        prepared.message.id,
                    );
                }
                Ok((prepared, delivery))
            })
            .await??;
        drop(target);
        let registry = Arc::clone(reg);
        let settled = platform::spawn(
            async move { registry.settle_delivery(prepared, delivery).await }.in_current_span(),
        );
        match settled.await {
            Ok(receipt) => receipt,
            Err(error) => Err(std::io::Error::other(error.to_string())),
        }
    })
}

#[inline(never)]
pub(super) fn observe_completions<'a>(
    reg: &'a Arc<Registry>,
    root_session_id: &'a str,
    ids: &'a [AgentId],
    call_id: &'a str,
    receipt: Option<Arc<ResultReceipt>>,
) -> RegistryFuture<'a, Option<Arc<dyn ToolReceipt>>> {
    Box::pin(async move {
        let resolver = Option::clone(&reg.parent_resolver.get());
        let Some(parent) = resolver.and_then(|resolver| resolver(root_session_id)) else {
            reg.acknowledge_observed(root_session_id, ids).await;
            return None;
        };
        let root = root_session_id.to_owned();
        let wanted = ids.to_vec();
        let pending = reg
            .state
            .call(move |state| {
                let scope = state.scopes.get(&root)?;
                Some(
                    wanted
                        .iter()
                        .filter_map(|id| {
                            let session = scope.sessions.get(id)?;
                            let revision = session.completion.revision()?;
                            session.status.wakes_parent().then_some((*id, revision))
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .await?;
        let (claimed, routed) = reg.completion_routes.update(|routes| {
            let mut claimed = Vec::new();
            let mut routed = Vec::new();
            for &(id, revision) in &pending {
                let key = (root_session_id.to_owned(), id);
                match routes.get(&key) {
                    None => {
                        routes.insert(key, revision);
                        claimed.push((id, revision));
                    }
                    Some(&held) if held == revision => routed.push(key),
                    Some(_) => {}
                }
            }
            (claimed, routed)
        });
        let observing_wait = receipt.is_some();
        let receipt = receipt
            .unwrap_or_else(|| Arc::new(ResultReceipt::new(WaitCall::direct(call_id), None)));
        let joined = reg.result_readers.read(|readers| {
            routed
                .iter()
                .filter_map(|key| readers.get(key))
                .map(|readers| readers.update(|readers| readers.push(Arc::clone(&receipt.reader))))
                .count()
        });
        if claimed.is_empty() {
            return (joined > 0 || observing_wait).then_some(receipt as Arc<dyn ToolReceipt>);
        }
        let watched = claimed
            .into_iter()
            .map(|claim| (claim, parent.subscribe()))
            .collect::<Vec<_>>();
        let running = reg
            .running_roots
            .read(|running| running.contains(root_session_id));
        for ((id, revision), events) in watched {
            let key = (root_session_id.to_owned(), id);
            let readers: Readers = Arc::new(Atom::new(vec![Arc::clone(&receipt.reader)]));
            reg.result_readers.update(|all| {
                all.insert(key.clone(), Arc::clone(&readers));
            });
            let registry = Arc::downgrade(reg);
            let parent = parent.downgrade();
            drop(platform::spawn(async move {
                let outcome = if running {
                    NoticeOutcome::of_tool_output(events, &readers, false).await
                } else {
                    NoticeOutcome::Undelivered
                };
                let Some(registry) = registry.upgrade() else {
                    return;
                };
                registry.result_readers.update(|all| {
                    if all
                        .get(&key)
                        .is_some_and(|current| Arc::ptr_eq(current, &readers))
                    {
                        all.remove(&key);
                    }
                });
                registry
                    .settle_notice(&key.0, id, revision, outcome, &parent)
                    .await;
            }));
        }
        Some(receipt)
    })
}

#[inline(never)]
pub(super) fn publish_message_state<'a>(
    reg: &'a Registry,
    root_session_id: &'a str,
    message_id: MessageId,
    delivery: MessageDeliveryState,
) -> RegistryFuture<'a, ()> {
    Box::pin(async move {
        let Some(registry) = reg.shared() else {
            return;
        };
        let root = root_session_id.to_owned();
        reg.state
            .call(move |state| {
                let Some(thread) = state.thread_for_message(&root, message_id) else {
                    return;
                };
                registry.send(
                    &root,
                    AgentUpdate::Message(AgentMessageUpdate {
                        message_id,
                        thread,
                        delivery,
                    }),
                );
                registry.changed();
                state.mark_message_terminal(&root, message_id);
            })
            .await;
    })
}

#[inline(never)]
pub(super) fn dispatch_parent<'a>(
    reg: &'a Arc<Registry>,
    parent: &'a Agent,
    root_session_id: &'a str,
    id: AgentId,
    revision: u64,
) -> RegistryFuture<'a, std::io::Result<Routed>> {
    Box::pin(async move {
        if parent.session_id() != root_session_id {
            return Err(std::io::Error::other(
                "subagent completion parent is not the root session",
            ));
        }
        let _warm = reg.warm(root_session_id, vec![id]).await?;
        let key = (root_session_id.to_owned(), id);
        if reg.route_claimed(root_session_id, id, revision) {
            return Ok(Routed::Skipped);
        }
        let Some((_, _, _, parked)) = reg.peek_completion(root_session_id, id, revision).await
        else {
            return Ok(Routed::Skipped);
        };
        if parked
            || reg
                .park_completion(root_session_id, id, revision, false)
                .await
        {
            reg.watch_root(root_session_id, parent);
            if !parked && reg.store_for(root_session_id).is_some() {
                reg.persist_durably(root_session_id).await?;
            }
            return Ok(Routed::Skipped);
        }
        if reg.store_for(root_session_id).is_some()
            && let Err(error) = reg.persist_durably(root_session_id).await
        {
            reg.retry_completion(root_session_id, id, revision);
            return Err(error);
        }
        if !reg.claim_route(root_session_id, id, revision) {
            return Ok(Routed::Skipped);
        }
        let Some((status, session_id, attempt, _)) =
            reg.peek_completion(root_session_id, id, revision).await
        else {
            reg.release_route(root_session_id, id, revision);
            return Ok(Routed::Skipped);
        };
        let request_id = completion_request_id(&session_id, revision, attempt);
        let prompt = completion_continuation_prompt(id, &status);
        let (steered, injected) = reg.watch_steer(root_session_id, parent, &request_id, &prompt);
        match parent
            .route_prompt_request(
                nanocode_agent_core::PromptRequest::new(prompt.clone())
                    .request_id(request_id.clone()),
            )
            .await
        {
            Ok(route) => {
                reg.report_notice(key.clone(), wait_mark(&status, revision));
                reg.completion_routes.update(|routes| {
                    routes.insert(key.clone(), revision);
                });
                let routed = match route {
                    PromptRoute::Started(_) => Routed::Started,
                    PromptRoute::Steered => Routed::Steered(Arc::clone(&injected)),
                };
                let registry = Arc::downgrade(reg);
                let parent = parent.downgrade();
                drop(platform::spawn(async move {
                    let outcome =
                        NoticeOutcome::of_route(route, steered, &parent, request_id, prompt).await;
                    injected.fire();
                    let Some(registry) = registry.upgrade() else {
                        return;
                    };
                    registry
                        .settle_notice(&key.0, id, revision, outcome, &parent)
                        .await;
                }));
                Ok(routed)
            }
            Err(error) => {
                reg.release_route(root_session_id, id, revision);
                if refused_input(&error) {
                    reg.next_completion_attempt(root_session_id, id, revision)
                        .await;
                }
                if !matches!(
                    error,
                    NanocodeError::AgentStopped | NanocodeError::TurnStopped
                ) {
                    reg.retry_completion(root_session_id, id, revision);
                }
                Err(std::io::Error::other(error))
            }
        }
    })
}

#[inline(never)]
pub(super) fn dispatch_parent_batch<'a>(
    reg: &'a Arc<Registry>,
    parent: &'a Agent,
    root_session_id: &'a str,
    mut completions: Vec<(AgentId, u64)>,
) -> RegistryFuture<'a, std::io::Result<Vec<Arc<Signal>>>> {
    Box::pin(async move {
        completions.sort_unstable_by(|left, right| left.0.cmp(&right.0).then(right.1.cmp(&left.1)));
        completions.dedup_by_key(|(id, _)| *id);
        if let [(id, revision)] = completions[..] {
            let result = reg
                .dispatch_parent(parent, root_session_id, id, revision)
                .await;
            return result.map(|routed| match routed {
                Routed::Steered(injected) => vec![injected],
                Routed::Skipped | Routed::Started => Vec::new(),
            });
        }
        let _warm = reg
            .warm(
                root_session_id,
                completions.iter().map(|(id, _)| *id).collect(),
            )
            .await?;
        let mut ready = Vec::new();
        let mut parked_any = false;
        for (id, revision) in completions {
            if reg.route_claimed(root_session_id, id, revision) {
                continue;
            }
            let Some((_, _, _, parked)) = reg.peek_completion(root_session_id, id, revision).await
            else {
                continue;
            };
            if parked
                || reg
                    .park_completion(root_session_id, id, revision, false)
                    .await
            {
                reg.watch_root(root_session_id, parent);
                parked_any |= !parked;
                continue;
            }
            ready.push((id, revision));
        }
        if reg.store_for(root_session_id).is_some()
            && (parked_any || !ready.is_empty())
            && let Err(error) = reg.persist_durably(root_session_id).await
        {
            for &(id, revision) in &ready {
                reg.retry_completion(root_session_id, id, revision);
            }
            return Err(error);
        }
        let mut groups = Vec::<(
            Option<Arc<str>>,
            Vec<(AgentId, u64, AgentStatus, String, u32)>,
        )>::new();
        for (id, revision) in ready {
            let Some((status, session_id, attempt, _)) =
                reg.peek_completion(root_session_id, id, revision).await
            else {
                continue;
            };
            let started = parent
                .unfinished_request(&completion_request_id(&session_id, revision, attempt))
                .await;
            if !matches!(started, Ok(false)) {
                groups.push((None, vec![(id, revision, status, session_id, attempt)]));
                continue;
            }
            let host_context = reg.host_context(root_session_id, id).await;
            match groups
                .iter_mut()
                .find(|(context, _)| *context == host_context)
            {
                Some((_, group)) => group.push((id, revision, status, session_id, attempt)),
                None => groups.push((
                    host_context,
                    vec![(id, revision, status, session_id, attempt)],
                )),
            }
        }
        let mut steered = Vec::new();
        for (_, group) in groups {
            steered.extend(reg.route_completions(parent, root_session_id, group).await);
        }
        Ok(steered)
    })
}

/// A completion request the parent can never admit under its current ID: an invalid request, or
/// a durable operation that already holds a different input. The second happens when the parent
/// ran this completion under an earlier status and the journal write acknowledging it was lost:
/// the retry carries the new status, so the same ID conflicts on every attempt. Both move the
/// completion to its next attempt's ID instead of retrying the refused one forever.
fn refused_input(error: &NanocodeError) -> bool {
    match error {
        NanocodeError::InvalidRequest(_) => true,
        NanocodeError::ExecutionPolicy { source, .. } => {
            source
                .downcast_ref::<NanocodeError>()
                .is_some_and(refused_input)
                || matches!(
                    source.downcast_ref::<nanocode_durability_core::Error>(),
                    Some(nanocode_durability_core::Error::OperationConflict { .. })
                )
        }
        NanocodeError::Shutdown(source) => refused_input(source),
        _ => false,
    }
}

#[inline(never)]
pub(super) fn route_completions<'a>(
    reg: &'a Arc<Registry>,
    parent: &'a Agent,
    root_session_id: &'a str,
    group: Vec<(AgentId, u64, AgentStatus, String, u32)>,
) -> RegistryFuture<'a, Option<Arc<Signal>>> {
    Box::pin(async move {
        let mut claimed = Vec::with_capacity(group.len());
        for (id, revision, status, session_id, attempt) in group {
            if !reg.claim_route(root_session_id, id, revision) {
                continue;
            }
            if reg
                .peek_completion(root_session_id, id, revision)
                .await
                .is_none()
            {
                reg.release_route(root_session_id, id, revision);
                continue;
            }
            claimed.push((id, revision, status, session_id, attempt));
        }
        let group = claimed;
        let (_, first_revision, _, first_session, _) = group.first()?;
        let request_id = if let [(_, revision, _, session_id, attempt)] = &group[..] {
            completion_request_id(session_id, *revision, *attempt)
        } else {
            let members = group
                .iter()
                .map(|(_, revision, _, session_id, attempt)| {
                    format!("{session_id}:{revision}:{attempt}")
                })
                .collect::<Vec<_>>()
                .join("\n");
            format!(
                "{}{first_session}:{first_revision}:batch-{}",
                crate::COMPLETION_REQUEST_PREFIX,
                durable::checkpoint_key(&members)
            )
        };
        let prompt = if let [(id, _, status, _, _)] = &group[..] {
            completion_continuation_prompt(*id, status)
        } else {
            completion_batch_prompt(
                &group
                    .iter()
                    .map(|(id, _, status, _, _)| (*id, status))
                    .collect::<Vec<_>>(),
            )
        };
        let (steered, injected) = reg.watch_steer(root_session_id, parent, &request_id, &prompt);
        match parent
            .route_prompt_request(
                nanocode_agent_core::PromptRequest::new(prompt.clone())
                    .request_id(request_id.clone()),
            )
            .await
        {
            Ok(route) => {
                for (id, revision, status, _, _) in &group {
                    let key = (root_session_id.to_owned(), *id);
                    reg.report_notice(key.clone(), wait_mark(status, *revision));
                    reg.completion_routes.update(|routes| {
                        routes.insert(key.clone(), *revision);
                    });
                }
                let routed = match route {
                    PromptRoute::Started(_) => None,
                    PromptRoute::Steered => Some(Arc::clone(&injected)),
                };
                let registry = Arc::downgrade(reg);
                let parent = parent.downgrade();
                let root_session_id = root_session_id.to_owned();
                drop(platform::spawn(async move {
                    let outcome =
                        NoticeOutcome::of_route(route, steered, &parent, request_id, prompt).await;
                    injected.fire();
                    let Some(registry) = registry.upgrade() else {
                        return;
                    };
                    for (id, revision, _, _, _) in group {
                        registry
                            .settle_notice(&root_session_id, id, revision, outcome, &parent)
                            .await;
                    }
                }));
                routed
            }
            Err(error) => {
                for (id, revision, _, _, _) in &group {
                    reg.release_route(root_session_id, *id, *revision);
                }
                if !matches!(
                    error,
                    NanocodeError::AgentStopped | NanocodeError::TurnStopped
                ) {
                    for (id, revision, _, _, _) in group {
                        if refused_input(&error) {
                            reg.next_completion_attempt(root_session_id, id, revision)
                                .await;
                        }
                        reg.retry_completion(root_session_id, id, revision);
                    }
                }
                nanocode_trace::warn!(%error, "subagent completions did not continue their parent");
                None
            }
        }
    })
}
