#!/usr/bin/env bash
set -u
here=$(cd "$(dirname "$0")" && pwd)
if [ -f "$here/results.jsonl" ]; then
  root=${NANOCODE_CFO_ROOT:-$here}
else
  root=${NANOCODE_CFO_ROOT:-$HOME/nanocode-cfo}
fi
remote=${NANOCODE_CFO_REMOTE:-https://github.com/nanocodexyz/nanocode}
branch=${NANOCODE_CFO_BRANCH:-master}
budget_secs=${NANOCODE_CFO_BUDGET_SECS:-3600}
refresh_secs=${NANOCODE_CFO_REFRESH_SECS:-300}
job_secs_target=${NANOCODE_CFO_JOB_SECS:-60}
job_secs_max=${NANOCODE_CFO_TIMEOUT_SECS:-1800}
fuzz_cpus=${NANOCODE_CFO_FUZZ_CPUS:-20-31}
build_cpus=${NANOCODE_CFO_BUILD_CPUS:-16-19}
build_jobs=${NANOCODE_CFO_BUILD_JOBS:-4}
db_remote=${NANOCODE_CFO_DB:-https://github.com/gakonst/nanocode-cfo-db}
host=${NANOCODE_CFO_HOST:-$(hostname -s)}
triage_secs=${NANOCODE_CFO_TRIAGE_SECS:-600}
canary_seeds=${NANOCODE_CFO_CANARY_SEEDS:-400}
memory_max=${NANOCODE_CFO_MEMORY_MAX:-32G}
commits_kept=32
failures_kept=4
upload_tries=32
log_bytes_max=$(( 4 << 20 ))
log_days=7
results_bytes_max=$(( 1 << 30 ))
triage_window_bytes=$(( 64 << 20 ))
upload_first_bytes=$(( 64 << 20 ))
failing_shown=3
message_chars=240
batch_first=20
seeds_min=1
seeds_max=1000000
seed_mask=$(( (1 << 62) - 1 ))
cpu_count() {
  local total=0 part
  for part in $(echo "$1" | tr ',' ' '); do
    total=$(( total + ${part#*-} - ${part%-*} + 1 ))
  done
  echo "$total"
}
workers=${NANOCODE_CFO_WORKERS:-$(cpu_count "$fuzz_cpus")}
results=$root/results.jsonl
logs=$root/failures
default_table='
16 store_content pass --scenario store
4 store_content_twice pass --scenario store --twice
16 store_slots pass --scenario store --records slots
4 store_slots_twice pass --scenario store --records slots --twice
16 agents pass --scenario agents
4 agents_twice pass --scenario agents --twice
8 turns_reference pass --scenario turns
8 turns_turnlog pass --scenario turns --policy turnlog
2 turns_turnlog_twice pass --scenario turns --policy turnlog --twice
2 turns_reference_calm pass --scenario turns --calm
2 turns_turnlog_row_limit pass --scenario turns --policy turnlog --row-limit 1024
8 runtime_memory pass --scenario runtime --store memory
8 runtime_kv pass --scenario runtime --store kv
2 runtime_memory_twice pass --scenario runtime --store memory --twice
2 runtime_kv_twice pass --scenario runtime --store kv --twice
2 runtime_kv_calm pass --scenario runtime --store kv --calm
8 runtime_kv_http pass --scenario runtime --store kv --wire http
4 runtime_memory_http pass --scenario runtime --store memory --wire http
2 runtime_kv_http_twice pass --scenario runtime --store kv --wire http --twice
8 websocket pass --scenario websocket
2 websocket_twice pass --scenario websocket --twice
8 http_server pass --scenario http_server
2 http_server_twice pass --scenario http_server --twice
8 egress pass --scenario egress
2 egress_twice pass --scenario egress --twice
8 mpp pass --scenario mpp
2 mpp_twice pass --scenario mpp --twice
8 sync pass --scenario sync
2 sync_twice pass --scenario sync --twice
8 streams pass --scenario streams
2 streams_twice pass --scenario streams --twice
1 canary_streams_drop_pipe_byte canary --scenario streams --mutation drop_pipe_byte
1 canary_streams_early_eof canary --scenario streams --mutation early_eof
1 canary_streams_skip_wake canary --scenario streams --mutation skip_wake
1 canary_streams_drop_spawned canary --scenario streams --mutation drop_spawned
1 canary_streams_corrupt_tls_byte canary --scenario streams --mutation corrupt_tls_byte
1 canary_streams_tls12_without_ed25519 canary --scenario streams --mutation tls12_without_ed25519
1 canary_streams_tls_write_after_error canary --scenario streams --mutation tls_write_after_error
1 canary_streams_tls_cache_by_capacity canary --scenario streams --mutation tls_cache_by_capacity
1 canary_streams_tls_encrypted_ccs canary --scenario streams --mutation tls_encrypted_ccs
1 canary_streams_tls_late_key_update canary --scenario streams --mutation tls_late_key_update
1 canary_cas_ignores_expected canary --scenario store --mutation cas_ignores_expected
1 canary_record_insert_drops canary --scenario store --mutation record_insert_drops
1 canary_power_loss_drops_acked canary --scenario store --mutation power_loss_drops_acked
1 canary_record_insert_overwrites canary --scenario store --records slots --mutation record_insert_overwrites
1 canary_cache_keeps_blank canary --scenario store --mutation cache_keeps_blank
1 canary_staged_refuses_rewrite canary --scenario store --mutation staged_refuses_rewrite
1 canary_durable_seq_before_commit canary --scenario turns --mutation durable_seq_before_commit
1 canary_finish_before_row canary --scenario turns --mutation finish_before_row
1 canary_gate_releases_early canary --scenario turns --mutation gate_releases_early
1 canary_idempotent_reported_unknown canary --scenario turns --mutation idempotent_reported_unknown
1 canary_recovery_skips_clientless canary --scenario turns --mutation recovery_skips_clientless
1 canary_tick_not_atomic canary --scenario turns --mutation tick_not_atomic
1 canary_seal_skipped canary:250 --scenario turns --live-seqs floor --mutation seal_skipped
1 canary_http_reuse_dirty canary:100 --scenario runtime --store kv --wire http --mutation http-reuse-dirty
1 canary_http_no_read_timeout canary:200 --scenario runtime --store kv --wire http --mutation http-no-read-timeout
1 canary_tool_result_twice canary:40 --scenario runtime --store kv --wire http --mutation tool-result-twice
1 canary_http_coding_passthrough canary:40 --scenario runtime --store kv --wire http --mutation http-coding-passthrough
1 canary_drop_cancelled_prompt canary:100 --scenario runtime --mutation drop-cancelled-prompt
1 canary_websocket_corrupt_payload canary --scenario websocket --mutation corrupt_payload
1 canary_websocket_drop_client_byte canary --scenario websocket --mutation drop_client_byte
1 canary_websocket_hold_eof canary --scenario websocket --mutation hold_eof
1 canary_http_server_corrupt_response_byte canary --scenario http_server --mutation corrupt_response_byte
1 canary_http_server_drop_request_byte canary --scenario http_server --mutation drop_request_byte
8 alloc pass --scenario alloc
2 alloc_twice pass --scenario alloc --twice
1 canary_alloc_overstate_size canary --scenario alloc --mutation overstate_size
1 canary_alloc_forget_free canary --scenario alloc --mutation forget_free
1 canary_alloc_scribble canary --scenario alloc --mutation scribble
1 canary_egress_widen_allowlist canary --scenario egress --mutation widen_allowlist
1 canary_egress_corrupt_tunnel_byte canary --scenario egress --mutation corrupt_tunnel_byte
1 canary_mpp_drop_abandon canary --scenario mpp --mutation drop_abandon
1 canary_mpp_receipt_on_refusal canary --scenario mpp --mutation receipt_on_refusal
1 canary_mpp_spent_save_regresses canary --scenario mpp --mutation spent_save_regresses
1 canary_mpp_reserve_skips_compare canary --scenario mpp --mutation reserve_skips_compare
1 canary_sync_skip_notify canary --scenario sync --mutation skip_notify
1 canary_sync_leak_permit canary --scenario sync --mutation leak_permit
1 canary_sync_reverse_queue canary --scenario sync --mutation reverse_queue
1 canary_sync_resend canary --scenario sync --mutation resend
1 canary_sync_shrink_capacity canary --scenario sync --mutation shrink_capacity
1 canary_sync_watch_rewind canary --scenario sync --mutation watch_rewind
1 canary_sync_broadcast_extra canary --scenario sync --mutation broadcast_extra
1 canary_sync_ring_off_by_one canary --scenario sync --mutation ring_off_by_one
1 canary_sync_fill_keeps_abandoned canary --scenario sync --mutation fill_keeps_abandoned
1 canary_sync_acquire_ignores_deadline canary --scenario sync --mutation acquire_ignores_deadline
4 turns_turnlog_events pass --scenario turns --policy turnlog --event-bytes 32768
4 turns_turnlog_snapshots pass --scenario turns --policy turnlog --snapshot-rows 4 --state-bytes 131072
2 turns_turnlog_retain pass --scenario turns --policy turnlog --retain 1
4 turns_turnlog_takeover pass --scenario turns --policy turnlog --snapshot-rows 4 --state-bytes 131072 --takeover
4 turns_turnlog_chain_bytes pass --scenario turns --policy turnlog --chain-bytes 32768 --state-bytes 131072
4 turns_turnlog_chain_bytes_snapshots pass --scenario turns --policy turnlog --chain-bytes 32768 --snapshot-rows 4 --state-bytes 131072
4 runtime_kv_remote pass --scenario runtime --store kv --children remote
1 runtime_kv_remote_twice pass --scenario runtime --store kv --children remote --twice
8 hands pass --scenario hands
2 hands_twice pass --scenario hands --twice
8 executor pass --scenario executor
2 executor_twice pass --scenario executor --twice
8 capture pass --scenario capture
2 capture_twice pass --scenario capture --twice
1 canary_ack_buffered_admission canary --scenario turns --policy turnlog --mutation ack_buffered_admission
1 canary_ack_buffered_result canary --scenario turns --policy turnlog --mutation ack_buffered_result
1 canary_archive_drops_events canary --scenario turns --policy turnlog --event-bytes 32768 --mutation archive_drops_events
1 canary_snapshot_kept canary --scenario turns --policy turnlog --snapshot-rows 4 --state-bytes 131072 --mutation snapshot_kept
1 canary_snapshot_rows_ignored canary --scenario turns --policy turnlog --snapshot-rows 4 --mutation snapshot_rows_ignored
1 canary_tail_rows_appended canary --scenario turns --policy turnlog --snapshot-rows 4 --mutation tail_rows_appended
1 canary_retire_skipped canary --scenario turns --policy turnlog --retain 1 --mutation retire_skipped
1 canary_takeover_opens canary --scenario turns --policy turnlog --snapshot-rows 4 --takeover --mutation takeover_opens
1 canary_hedge_unmarked canary:200 --scenario runtime --mutation hedge-unmarked
1 canary_http_real_elapsed canary:40 --scenario runtime --store kv --wire http --twice --mutation http-real-elapsed
1 canary_child_acks_early canary:1000 --scenario runtime --children remote --mutation child-acks-early
1 canary_child_adopts_turn canary:400 --scenario runtime --children remote --mutation child-adopts-turn
4 runtime_log pass --scenario runtime --store log
4 runtime_log_board pass --scenario runtime --store log --subagents-board
1 runtime_board_pinned_claim_terminal pass --scenario runtime --subagents-board --seed 1795172135120000028
1 runtime_kv_remote_pinned_close_message pass --scenario runtime --store kv --children remote --seed 559288643416904524
1 runtime_kv_remote_pinned_close_cancel pass --scenario runtime --store kv --children remote --seed 557886671430719270
1 runtime_kv_remote_pinned_close_start pass --scenario runtime --store kv --children remote --seed 7300000000000016081
4 agents_board pass --scenario agents --subagents-board
4 hands_fleet pass --scenario hands --fleet 4
1 canary_coordinator_forgets_spawns canary:200 --scenario runtime --children remote --mutation coordinator-forgets-spawns
1 canary_acks_unflushed canary:400 --scenario runtime --store log --mutation acks-unflushed
1 canary_reopen_forgets_agents canary:100 --scenario runtime --store log --subagents-board --mutation reopen-forgets-agents
1 canary_drop_fact_decision canary:20 --scenario runtime --store log --mutation drop-fact-decision
1 canary_board_failures_unread canary:100 --scenario agents --subagents-board --mutation board_failures_unread
1 canary_fleet_swallow_unsent canary:250 --scenario hands --fleet 4 --mutation fleet_swallow_unsent
1 canary_child_forgets_notice canary:100 --scenario runtime --children remote --mutation child-forgets-notice
1 canary_child_respawns_fresh canary:400 --scenario runtime --children remote --mutation child-respawns-fresh
1 canary_child_settles_lazily canary:100 --scenario runtime --children remote --mutation child-settles-lazily
1 canary_hands_ack_before_store canary:100 --scenario hands --mutation ack_before_store
1 canary_hands_skip_record canary:200 --scenario hands --mutation skip_record
1 canary_hands_forget_pending canary:200 --scenario hands --mutation forget_pending
1 canary_hands_acceptor_drops_selection canary:8 --scenario hands --mutation acceptor_drops_selection
1 canary_hands_cross_replies canary:200 --scenario hands --mutation cross_replies
1 canary_hands_mangle_runtime canary:200 --scenario hands --mutation mangle_runtime
1 canary_hands_reply_overwrites canary:40 --scenario hands --mutation reply_overwrites
1 canary_hands_resume_waiting_only canary:350 --scenario hands --mutation resume_waiting_only
8 workspace_stack pass --scenario workspace_stack
2 workspace_stack_twice pass --scenario workspace_stack --twice
1 canary_workspace_stack_torn_commit canary:200 --scenario workspace_stack --mutation torn_commit
1 canary_workspace_stack_visible_before_commit canary:200 --scenario workspace_stack --mutation visible_before_commit
1 canary_workspace_stack_answers_now canary:200 --scenario workspace_stack --mutation answers_now
1 canary_workspace_stack_has_nothing canary:200 --scenario workspace_stack --mutation has_nothing
1 canary_workspace_stack_has_everything canary:200 --scenario workspace_stack --mutation has_everything
1 canary_workspace_stack_auto_off canary:200 --scenario workspace_stack --mutation auto_off
4 git_remote pass --scenario git_remote
1 git_remote_twice pass --scenario git_remote --twice
1 canary_git_remote_stores_secret canary:200 --scenario git_remote --mutation stores_secret
1 canary_git_remote_updates_ref_on_bad_pack canary:200 --scenario git_remote --mutation updates_ref_on_bad_pack
1 canary_git_remote_server_drops_object canary:200 --scenario git_remote --mutation server_drops_object
1 canary_git_remote_server_keeps_ref canary:200 --scenario git_remote --mutation server_keeps_ref
1 canary_git_remote_stale_tracking canary:200 --scenario git_remote --mutation stale_tracking
1 canary_git_remote_push_wrong_base canary:200 --scenario git_remote --mutation push_wrong_base
1 canary_git_remote_forgets_shallow canary:200 --scenario git_remote --mutation forgets_shallow
1 canary_git_remote_keeps_pruned canary:200 --scenario git_remote --mutation keeps_pruned
1 canary_git_remote_forces_refspec canary:200 --scenario git_remote --mutation forces_refspec
1 canary_git_remote_reverts_upstream canary:200 --scenario git_remote --mutation reverts_upstream
4 workspace_crash pass --scenario workspace_crash
1 workspace_crash_twice pass --scenario workspace_crash --twice
1 canary_workspace_crash_torn_commit canary:200 --scenario workspace_crash --mutation torn_commit
1 canary_workspace_crash_backend_failure_acked canary:200 --scenario workspace_crash --mutation backend_failure_acked
1 canary_workspace_crash_visible_before_commit canary:200 --scenario workspace_crash --mutation visible_before_commit
1 canary_workspace_crash_lost_update canary:200 --scenario workspace_crash --mutation lost_update
1 canary_workspace_crash_stale_retry_overwrites canary:200 --scenario workspace_crash --mutation stale_retry_overwrites
1 canary_workspace_crash_stale_surfaces canary:200 --scenario workspace_crash --mutation stale_surfaces
1 canary_workspace_crash_seq_rename_copies canary:200 --scenario workspace_crash --mutation seq_rename_copies
1 canary_workspace_crash_held_redirect_appends canary:200 --scenario workspace_crash --mutation held_redirect_appends
8 workspace_sql pass --scenario workspace_sql
1 canary_workspace_sql_stale_read_skips_reload canary:400 --scenario workspace_sql --mutation stale_read_skips_reload
1 canary_workspace_sql_sweep_ignores_refs canary:200 --scenario workspace_sql --mutation sweep_ignores_refs
1 canary_workspace_sql_open_migrates canary:8 --scenario workspace_sql --mutation open_migrates
1 canary_executor_skip_abort canary --scenario executor --mutation skip_abort
1 canary_executor_leak_cancelled canary --scenario executor --mutation leak_cancelled
1 canary_executor_uncounted_polls canary:100 --scenario executor --mutation uncounted_polls
1 canary_executor_unchecked_drive canary:40 --scenario executor --mutation unchecked_drive
1 canary_executor_stale_waiters canary:100:21 --scenario executor --mutation stale_waiters
1 canary_executor_strands_refused canary --scenario executor --mutation strands_refused
1 canary_executor_refuses_served canary --scenario executor --mutation refuses_served
8 board pass --scenario board
2 board_twice pass --scenario board --twice
1 board_pinned_squatted_ids pass --scenario board --seed 1795147396000000556
1 board_pinned_join_root pass --scenario board --seed 1208986242872782921
1 board_pinned_watch_drain pass --scenario board --seed 95701
1 board_shards_pinned_short_lease pass --scenario board_shards --seed 156486433204944099
1 board_shards_pinned_checkpoint_host pass --scenario board_shards --seed 42004
1 canary_rate_limit_counts_attempt canary:100 --scenario board_sync --mutation rate_limit_counts_attempt
1 board_shards_pinned_lapse_budget pass --scenario board_shards --seed 1795200002000022054
4 board_steer pass --scenario board_steer
2 board_steer_twice pass --scenario board_steer --twice
1 canary_board_steer_lost_on_move canary:40 --scenario board_steer --mutation steer_lost_on_move
1 canary_board_steer_redelivered_after_heed canary:40 --scenario board_steer --mutation steer_redelivered_after_heed
1 canary_board_heed_from_stale_claim canary:40 --scenario board_steer --mutation heed_from_stale_claim
1 canary_board_leases_never_expire canary:50 --scenario board --mutation leases_never_expire
1 canary_board_move_allows_cycle canary:400 --scenario board --mutation move_allows_cycle
1 canary_board_unschedule_ignored canary:20 --scenario board --mutation unschedule_ignored
1 canary_board_checkpoint_version_ignored canary:20 --scenario board --mutation checkpoint_version_ignored
1 canary_board_checkpoint_tail_unchecked canary:20 --scenario board --mutation checkpoint_tail_unchecked
1 canary_board_checkpoint_ahead canary:20 --scenario board --mutation checkpoint_ahead
1 canary_board_checkpoint_every_fixed canary:50 --scenario board --mutation checkpoint_every_fixed
1 canary_board_eviction_differs_by_replica canary:50 --scenario board --mutation eviction_differs_by_replica
1 canary_board_events_uncapped canary:50 --scenario board --mutation events_uncapped
1 canary_board_gone_unbounded canary:50 --scenario board --mutation gone_unbounded
1 canary_board_steer_by_claim_follows canary:40 --scenario board_steer --mutation steer_by_claim_follows
1 canary_board_steer_to_terminal canary:40 --scenario board_steer --mutation steer_to_terminal
1 canary_board_finish_ignores_steers canary:40 --scenario board_steer --mutation finish_ignores_steers
1 canary_board_steer_kept_on_cancel canary:40 --scenario board_steer --mutation steer_kept_on_cancel
1 canary_board_step_takes_steers_twice canary:40 --scenario board_steer --mutation step_takes_steers_twice
1 canary_board_step_heeds_unseen_steers canary:40 --scenario board_steer --mutation step_heeds_unseen_steers
4 steer_carry pass --scenario steer_carry
2 steer_carry_twice pass --scenario steer_carry --twice
1 canary_steer_carry_as_text canary:40 --scenario steer_carry --mutation carry_steer_as_text
1 canary_steer_carry_ignores_earlier_read canary:40 --scenario steer_carry --mutation carry_ignores_earlier_read
1 canary_steer_carry_ignores_pruned_receipts canary:40 --scenario steer_carry --mutation carry_ignores_pruned_receipts
1 canary_steer_carry_codex_fact_dropped canary:20 --scenario steer_carry --mutation codex_fact_dropped
1 canary_steer_carry_gate_dropped canary:8 --scenario steer_carry --mutation carry_gate_dropped
1 canary_steer_waits_round canary:8 --scenario steer_carry --mutation steer_waits_round
4 antigravity_durable pass --scenario antigravity_durable
2 antigravity_durable_twice pass --scenario antigravity_durable --twice
1 canary_antigravity_moved_forgets_results canary:8 --scenario antigravity_durable --mutation antigravity_moved_forgets_results
1 canary_antigravity_replay_reruns canary:8 --scenario antigravity_durable --mutation antigravity_replay_reruns
1 canary_antigravity_fact_sync_skipped canary:8 --scenario antigravity_durable --mutation antigravity_fact_sync_skipped
8 board_brief pass --scenario board_brief
2 board_brief_twice pass --scenario board_brief --twice
1 canary_board_brief_over_budget canary:40 --scenario board_brief --mutation brief_over_budget
1 canary_board_brief_drops_steer canary:40 --scenario board_brief --mutation brief_drops_steer
1 canary_board_brief_leaks_parent_history canary:40 --scenario board_brief --mutation brief_leaks_parent_history
8 dispatcher pass --scenario dispatcher
2 dispatcher_twice pass --scenario dispatcher --twice
1 canary_dispatcher_starts_before_intent canary:100 --scenario dispatcher --mutation dispatcher_starts_before_intent
1 canary_dispatcher_retire_keeps_claims canary:40 --scenario dispatcher --mutation dispatcher_retire_keeps_claims
1 canary_dispatcher_sleeps_through_idle canary:40 --scenario dispatcher --mutation dispatcher_sleeps_through_idle
1 canary_dispatcher_sleeps_after_acting canary:40 --scenario dispatcher --mutation dispatcher_sleeps_after_acting
1 canary_dispatcher_never_reaps canary:40 --scenario dispatcher --mutation dispatcher_never_reaps
4 board_shards pass --scenario board_shards
2 board_shards_twice pass --scenario board_shards --twice
1 canary_board_shards_group_skips_seq canary:40 --scenario board_shards --mutation group_skips_seq
1 canary_board_shards_group_verdict_from_plan canary:40 --scenario board_shards --mutation group_verdict_from_plan
1 canary_board_shards_group_splits canary:40 --scenario board_shards --mutation group_splits
1 canary_board_shards_route_ignores_task canary:40 --scenario board_shards --mutation route_ignores_task
1 canary_board_shards_merge_skips_partition canary:40 --scenario board_shards --mutation merge_skips_partition
1 canary_board_shards_merged_index_skips_rows canary:40 --scenario board_shards --mutation merged_index_skips_rows
1 canary_board_shards_merged_index_ignores_rank canary:40 --scenario board_shards --mutation merged_index_ignores_rank
1 canary_board_shards_child_follows_parent canary:40 --scenario board_shards --mutation child_follows_parent
1 canary_board_shards_cross_move_keeps_parent canary:40 --scenario board_shards --mutation cross_move_keeps_parent
1 canary_board_shards_cross_move_skips_unmark canary:40 --scenario board_shards --mutation cross_move_skips_unmark
1 canary_board_shards_cross_move_ingests_first canary:40 --scenario board_shards --mutation cross_move_ingests_first
1 canary_board_shards_epoch_fence_ignored canary:40 --scenario board_shards --mutation epoch_fence_ignored
1 canary_board_shards_claim_cap_ignored canary:40 --scenario board_shards --mutation claim_cap_ignored
1 canary_board_shards_claim_cap_ignores_label canary:40 --scenario board_shards --mutation claim_cap_ignores_label
1 canary_board_shards_finish_forgets_worker canary:40 --scenario board_shards --mutation finish_forgets_worker
1 canary_board_shards_renew_ignores_cap canary:40 --scenario board_shards --mutation renew_ignores_cap
1 canary_capture_skip_trim canary --scenario capture --mutation skip_trim
1 canary_capture_drop_chunk canary --scenario capture --mutation drop_chunk
1 canary_buffered_admission_written canary --scenario turns --policy turnlog --mutation buffered_admission_written
1 canary_cell_before_checkpoint canary --scenario turns --policy turnlog --mutation cell_before_checkpoint
1 canary_open_drops_journals canary --scenario turns --policy turnlog --mutation open_drops_journals
1 canary_resume_redispatches_running canary --scenario turns --policy turnlog --mutation resume_redispatches_running
1 canary_snapshot_ratio_ignored canary:100 --scenario turns --policy turnlog --event-bytes 4096 --mutation snapshot_ratio_ignored
1 canary_snapshot_pointer_stale canary:100 --scenario turns --policy turnlog --mutation snapshot_pointer_stale
1 canary_sections_pruned canary:100 --scenario turns --policy turnlog --snapshot-rows 4 --mutation sections_pruned
1 canary_sections_kept canary:100 --scenario turns --policy turnlog --event-bytes 4096 --mutation sections_kept
1 canary_part_read_unloaded canary:100 --scenario turns --policy turnlog --snapshot-rows 4 --mutation part_read_unloaded
1 canary_forgets_other_cell canary:20 --scenario turns --policy turnlog --mutation forgets_other_cell
1 canary_cell_roots_skipped canary:20 --scenario turns --policy turnlog --snapshot-rows 4 --mutation cell_roots_skipped
1 canary_content_rewritten_stale canary:20 --scenario turns --policy turnlog --mutation content_rewritten_stale
1 canary_content_collected_live canary:20 --scenario turns --policy turnlog --mutation content_collected_live
1 canary_placed_inline canary:20 --scenario turns --policy turnlog --mutation placed_inline
1 canary_chain_member_dropped canary:20 --scenario turns --policy turnlog --snapshot-rows 4 --state-bytes 131072 --mutation chain_member_dropped
1 canary_fenced_serves_reads canary:20 --scenario turns --policy turnlog --snapshot-rows 4 --state-bytes 131072 --takeover --mutation fenced_serves_reads
1 canary_host_unbounded canary:20 --scenario turns --policy turnlog --snapshot-rows 4 --mutation host_unbounded
1 canary_host_bound_ignored canary:20 --scenario turns --policy turnlog --snapshot-rows 4 --mutation host_bound_ignored
1 canary_reads_unbounded canary:20 --scenario turns --policy turnlog --snapshot-rows 4 --mutation reads_unbounded
1 canary_answers_unbounded canary:20 --scenario turns --policy turnlog --snapshot-rows 4 --mutation answers_unbounded
1 canary_resident_until_snapshot canary:20 --scenario turns --policy turnlog --snapshot-rows 4 --mutation resident_until_snapshot
1 canary_unindexed_pins_pages canary:20 --scenario turns --policy turnlog --snapshot-rows 4 --mutation unindexed_pins_pages
1 canary_placed_stay_resident canary:20 --scenario turns --policy turnlog --snapshot-rows 4 --mutation placed_stay_resident
1 canary_scans_unbatched canary:20 --scenario turns --policy turnlog --snapshot-rows 4 --mutation scans_unbatched
1 canary_finished_keeps_prompt canary:20 --scenario turns --policy turnlog --mutation finished_keeps_prompt
1 canary_agents_cancel_ignored canary:200 --scenario agents --mutation cancel_ignored
1 canary_agents_shutdown_hangs canary:200 --scenario agents --mutation shutdown_hangs
1 canary_agents_shutdown_keeps_harness canary:200 --scenario agents --mutation shutdown_keeps_harness
1 canary_agents_pool_retire_keeps_agent canary:40 --scenario agents --mutation pool_retire_keeps_agent
1 canary_agents_pool_forgets_intent canary:40 --scenario agents --mutation pool_forgets_intent
1 canary_agents_wait_repeats_ids canary:40 --scenario agents --mutation wait_repeats_ids
1 canary_agents_restore_remembers_waits canary:40 --scenario agents --mutation restore_remembers_waits
1 canary_agents_route_skips_claim canary:40 --scenario agents --mutation route_skips_claim
1 canary_agents_scope_jobs_overlap canary:40 --scenario agents --mutation scope_jobs_overlap
1 canary_agents_restore_shares_claim canary:40 --scenario agents --mutation restore_shares_claim
1 canary_agents_scope_jobs_stop_inline canary:40 --scenario agents --mutation scope_jobs_stop_inline
1 canary_agents_scope_locks_kept canary:40 --scenario agents --mutation scope_locks_kept
1 canary_agents_board_pool_forgets_intent canary:40 --scenario agents --subagents-board --mutation pool_forgets_intent
4 agents_subagents_board_delayed pass --scenario agents --subagents-board-delayed
4 agents_subagents_board_lagging pass --scenario agents --subagents-board-lagging
1 agents_board_lagging_pinned_placement pass --scenario agents --subagents-board-lagging --seed 5
4 runtime_subagents_board_delayed pass --scenario runtime --subagents-board-delayed
4 runtime_subagents_board_lagging pass --scenario runtime --subagents-board-lagging
1 runtime_board_lagging_pinned_phase pass --scenario runtime --subagents-board-lagging --seed 1795200000000686281
1 runtime_board_lagging_pinned_cell_head pass --scenario runtime --subagents-board-lagging --seed 1795200000000152282
1 canary_subagents_board_polled_once canary:40 --scenario agents --subagents-board-mutation polled_once
1 canary_subagents_board_append_unflushed canary:40 --scenario agents --subagents-board-mutation append_unflushed
1 canary_subagents_board_reopen_lost canary:100 --scenario runtime --subagents-board-mutation reopen_lost
1 canary_subagents_board_steers_taken_mid_turn canary:40 --scenario agents --subagents-board-mutation steers_taken_mid_turn
1 canary_subagents_board_steers_never_delivered canary:40 --scenario agents --subagents-board-mutation steers_never_delivered
1 canary_subagents_board_refresh_once canary:40 --scenario runtime --subagents-board-mutation refresh_once
1 canary_board_fenced_unsettled canary:40 --scenario board --mutation fenced_unsettled
1 canary_runtime_kv_power_loss_drops_acked canary:100 --scenario runtime --store kv --mutation power_loss_drops_acked
1 canary_runtime_kv_power_loss_drops_acceptance canary:100 --scenario runtime --store kv --mutation power_loss_drops_acceptance
1 canary_runtime_kv_subagent_journal_keeps_revision canary:100:31 --scenario runtime --store kv --mutation subagent_journal_keeps_revision
1 canary_runtime_kv_cell_journal_keeps_head canary:200:17 --scenario runtime --store kv --mutation cell_journal_keeps_head
1 canary_runtime_kv_record_insert_drops canary:100 --scenario runtime --store kv --mutation record_insert_drops
1 canary_capture_redact_then_trim canary:4000 --scenario capture --mutation redact_then_trim
8 cell_calls pass --scenario cell_calls
2 cell_calls_twice pass --scenario cell_calls --twice
1 canary_cell_calls_cross_results canary:8 --scenario cell_calls --mutation cross_results
1 canary_cell_calls_root_recovery_unbound canary:8 --scenario cell_calls --mutation root_recovery_unbound
1 canary_cell_calls_admission_held canary:8 --scenario cell_calls --mutation admission_held
1 canary_cell_calls_admits_stale canary:8 --scenario cell_calls --mutation admits_stale
1 canary_cell_calls_observer_save_held canary:8 --scenario cell_calls --mutation observer_save_held
4 nested_meta pass --scenario nested_meta
1 nested_meta_twice pass --scenario nested_meta --twice
1 canary_nested_meta_forgets_written canary:8 --scenario nested_meta --mutation forgets_written
1 canary_nested_meta_written_on_failure canary:8 --scenario nested_meta --mutation written_on_failure
1 canary_nested_meta_catalog_keeps_escalation canary:8 --scenario nested_meta --mutation catalog_keeps_escalation
1 canary_nested_meta_call_keeps_escalation canary:8 --scenario nested_meta --mutation call_keeps_escalation
1 canary_nested_meta_unserved_sections_kept canary:8 --scenario nested_meta --mutation unserved_sections_kept
4 gc_unwind pass --scenario gc_unwind
1 gc_unwind_twice pass --scenario gc_unwind --twice
1 canary_gc_unwind_keeps_leftover_bits canary:8 --scenario gc_unwind --mutation keeps_leftover_bits
1 canary_gc_unwind_drop_skips_finalize canary:8 --scenario gc_unwind --mutation drop_skips_finalize
1 canary_gc_unwind_subclasses_keep_dead canary:8 --scenario gc_unwind --mutation subclasses_keep_dead
1 canary_gc_unwind_defs_per_interpreter canary:8 --scenario gc_unwind --mutation defs_per_interpreter
1 canary_gc_unwind_prunes_every_push canary:8 --scenario gc_unwind --mutation prunes_every_push
1 canary_gc_unwind_prunes_at_powers_of_two canary:8 --scenario gc_unwind --mutation prunes_at_powers_of_two
1 canary_gc_unwind_drop_runs_code canary:8 --scenario gc_unwind --mutation drop_runs_code
1 canary_gc_unwind_drop_lets_panic_out canary:8 --scenario gc_unwind --mutation drop_lets_panic_out
1 canary_gc_unwind_drop_finalizes_twice canary:8 --scenario gc_unwind --mutation drop_finalizes_twice
1 canary_gc_unwind_drop_finalizes_while_unwinding canary:8 --scenario gc_unwind --mutation drop_finalizes_while_unwinding
4 interp_drop pass --scenario interp_drop
1 interp_drop_twice pass --scenario interp_drop --twice
1 canary_interp_drop_hides_function_builtins canary:8 --scenario interp_drop --mutation hides_function_builtins
8 holds pass --scenario holds
2 holds_twice pass --scenario holds --twice
1 canary_holds_forgets_answer canary:8 --scenario holds --mutation forgets_answer
1 canary_holds_resume_ignores_answer canary:8 --scenario holds --mutation resume_ignores_answer
1 canary_holds_prunes_unsettled canary:20 --scenario holds --mutation prunes_unsettled
4 pg_pool pass --scenario pg_pool
1 pg_pool_twice pass --scenario pg_pool --twice
1 canary_pg_pool_waits_unbounded canary:8 --scenario pg_pool --mutation waits_unbounded
1 canary_pg_pool_call_bound_skips_cancel canary:20 --scenario pg_pool --mutation call_bound_skips_cancel
8 slog pass --scenario slog
2 slog_twice pass --scenario slog --twice
1 canary_slog_append_skips_epoch canary:8 --scenario slog --mutation append_skips_epoch
8 append pass --scenario append
2 append_twice pass --scenario append --twice
1 canary_append_retry_skips_read_back canary:32 --scenario append --mutation retry_skips_read_back
1 canary_append_conflict_skips_read_back canary:32 --scenario append --mutation conflict_skips_read_back
8 one_log_facts pass --scenario one_log_facts
2 one_log_facts_twice pass --scenario one_log_facts --twice
1 canary_one_log_facts_fold_reads_unlogged canary:8 --scenario one_log_facts --mutation fold_reads_unlogged
1 canary_one_log_facts_commit_buffered canary:20 --scenario one_log_facts --mutation commit_buffered
1 canary_one_log_facts_unseen_skipped canary:8 --scenario one_log_facts --mutation unseen_skipped
1 canary_one_log_facts_intent_replayed canary:20 --scenario one_log_facts --mutation intent_replayed
1 canary_one_log_facts_intent_copied canary:8 --scenario one_log_facts --mutation intent_copied
1 canary_one_log_facts_scope_leaks canary:8 --scenario one_log_facts --mutation scope_leaks
1 canary_one_log_facts_admission_copied canary:8 --scenario one_log_facts --mutation admission_copied
1 canary_one_log_facts_cache_gap canary:8 --scenario one_log_facts --mutation cache_gap
1 canary_one_log_facts_remembered_skips_tail canary:8 --scenario one_log_facts --mutation remembered_skips_tail
1 canary_one_log_facts_cache_never_due canary:8 --scenario one_log_facts --mutation cache_never_due
1 canary_one_log_facts_host_copied canary:8 --scenario one_log_facts --mutation host_copied
1 canary_one_log_facts_cancel_leaves_intent canary:8 --scenario one_log_facts --mutation cancel_leaves_intent
1 canary_one_log_facts_scoped_input_lowered canary:8 --scenario one_log_facts --mutation scoped_input_lowered
1 canary_one_log_facts_intent_optional canary:8 --scenario one_log_facts --mutation intent_optional
1 canary_one_log_facts_abandon_unsettled canary:8 --scenario one_log_facts --mutation abandon_unsettled
1 canary_one_log_facts_cache_inline canary:8 --scenario one_log_facts --mutation cache_inline
4 migrated pass --scenario migrated
1 migrated_twice pass --scenario migrated --twice
1 canary_migrated_open_migrates canary:32 --scenario migrated --mutation open_migrates
1 canary_migrated_check_misses_formats canary:32 --scenario migrated --mutation check_misses_formats
1 canary_migrated_prepare_migrates canary:32 --scenario migrated --mutation prepare_migrates
1 canary_migrated_concurrent_skips_lock_timeout canary:32 --scenario migrated --mutation concurrent_skips_lock_timeout
4 kv_lease pass --scenario kv_lease
1 kv_lease_twice pass --scenario kv_lease --twice
1 canary_kv_lease_claim_steals_live canary:16 --scenario kv_lease --mutation claim_steals_live
1 canary_kv_lease_claim_skips_grace canary:16 --scenario kv_lease --mutation claim_skips_grace
4 scope pass --scenario scope
1 scope_twice pass --scenario scope --twice
1 canary_scope_checkpoint_indexes_only canary:16 --scenario scope --mutation checkpoint_indexes_only
1 canary_scope_read_through_writer canary:16 --scenario scope --mutation read_through_writer
1 canary_scope_publish_before_commit canary:16 --scenario scope --mutation publish_before_commit
1 canary_scope_checkpoint_skips_epoch canary:16 --scenario scope --mutation checkpoint_skips_epoch
1 canary_scope_evict_without_checkpoint canary:16 --scenario scope --mutation evict_without_checkpoint
1 canary_scope_checkpoint_rewrites_unchanged_parts canary:16 --scenario scope --mutation checkpoint_rewrites_unchanged_parts
1 canary_scope_forward_waits_unbounded canary:16 --scenario scope --mutation forward_waits_unbounded
1 canary_scope_listing_chain_unbounded canary:16 --scenario scope --mutation listing_chain_unbounded
1 canary_scope_deferred_redecided canary:16 --scenario scope --mutation deferred_redecided
1 canary_scope_staged_published canary:16 --scenario scope --mutation staged_published
1 canary_scope_flush_skips_staged canary:16 --scenario scope --mutation flush_skips_staged
1 canary_scope_rider_split canary:16 --scenario scope --mutation rider_split
4 thread_scope pass --scenario thread_scope
1 thread_scope_twice pass --scenario thread_scope --twice
1 canary_thread_scope_part_dropped canary:16 --scenario thread_scope --mutation part_dropped
1 canary_thread_scope_listing_skips_dropped canary:16 --scenario thread_scope --mutation listing_skips_dropped
1 canary_thread_scope_adoption_skips_retired canary:16 --scenario thread_scope --mutation adoption_skips_retired
1 canary_thread_scope_adoption_skips_blobs canary:16 --scenario thread_scope --mutation adoption_skips_blobs
1 canary_thread_scope_adoption_skips_checkpoint canary:16 --scenario thread_scope --mutation adoption_skips_checkpoint
1 canary_thread_scope_job_forwarded canary:16 --scenario thread_scope --mutation job_forwarded
1 canary_thread_scope_job_rows_dropped canary:16 --scenario thread_scope --mutation job_rows_dropped
1 canary_thread_scope_scoped_intent_skipped canary:16 --scenario thread_scope --mutation scoped_intent_skipped
1 canary_thread_scope_facts_unseen_skipped canary:16 --scenario thread_scope --mutation facts_unseen_skipped
1 canary_thread_scope_recovery_uncounted canary:16 --scenario thread_scope --mutation recovery_uncounted
1 canary_thread_scope_snapshot_resets_recovery canary:256 --scenario thread_scope --mutation snapshot_resets_recovery
1 canary_thread_scope_hold_staged canary:16 --scenario thread_scope --mutation hold_staged
1 canary_thread_scope_wakes_unbounded canary:16 --scenario thread_scope --mutation wakes_unbounded
1 canary_thread_scope_wake_index_skipped canary:16 --scenario thread_scope --mutation wake_index_skipped
1 canary_thread_scope_gate_compares_intent_to_itself canary:16 --scenario thread_scope --mutation gate_compares_intent_to_itself
1 canary_thread_scope_lost_unretried canary:16 --scenario thread_scope --mutation lost_unretried
1 canary_thread_scope_store_lost_unretried canary:16 --scenario thread_scope --mutation store_lost_unretried
1 canary_thread_scope_journal_lost_unretried canary:16 --scenario thread_scope --mutation journal_lost_unretried
1 canary_thread_scope_keep_all_events canary:16 --scenario thread_scope --mutation keep_all_events
1 canary_thread_scope_keep_finished_turns canary:16 --scenario thread_scope --mutation keep_finished_turns
1 canary_thread_scope_admission_skips_history canary:16 --scenario thread_scope --mutation admission_skips_history
1 canary_thread_scope_scope_writes_untagged canary:16 --scenario thread_scope --mutation scope_writes_untagged
1 canary_thread_scope_scope_starts_at_row_zero canary:16 --scenario thread_scope --mutation scope_starts_at_row_zero
1 canary_thread_scope_reservation_skipped canary:16 --scenario thread_scope --mutation reservation_skipped
1 canary_thread_scope_reimport_advances canary:16 --scenario thread_scope --mutation reimport_advances
1 canary_thread_scope_store_skips_fence canary:16 --scenario thread_scope --mutation store_skips_fence
1 canary_thread_scope_journal_loads_finished canary:16 --scenario thread_scope --mutation journal_loads_finished
8 repo_cache pass --scenario repo_cache
2 repo_cache_twice pass --scenario repo_cache --twice
1 canary_repo_cache_shared_head canary:8 --scenario repo_cache --mutation shared_head
1 canary_repo_cache_fetch_moves_heads canary:8 --scenario repo_cache --mutation fetch_moves_heads
1 canary_repo_cache_refs_before_objects canary:8 --scenario repo_cache --mutation refs_before_objects
1 canary_repo_cache_resend_held canary:8 --scenario repo_cache --mutation resend_held
1 canary_repo_cache_shared_lock canary:8 --scenario repo_cache --mutation shared_lock
1 canary_plog_unfenced canary:50 --scenario plog --mutation unfenced
1 canary_plog_partial canary:50 --scenario plog --mutation partial
1 canary_plog_overwrite canary:50 --scenario plog --mutation overwrite
1 canary_plog_batch_duplicates canary:100 --scenario plog --mutation batch_duplicates
1 canary_plog_missed_ring canary:100 --scenario plog --mutation missed_ring
1 canary_plog_snapshot_skipped canary:50 --scenario plog --mutation snapshot_skipped
1 canary_plog_prune_ignores_cursors canary:400 --scenario plog --mutation prune_ignores_cursors
1 canary_plog_lost_ack_rejected canary:100 --scenario plog --mutation lost_ack_rejected
1 canary_plog_drain_refuses_queued canary:50 --scenario plog --mutation drain_refuses_queued
1 canary_plog_gain_waits_unbounded canary:50 --scenario plog --mutation gain_waits_unbounded
8 wakes pass --scenario wakes
2 wakes_twice pass --scenario wakes --twice
1 canary_wakes_fired_apart canary:100 --scenario wakes --mutation fired_apart
1 canary_wakes_snapshot_drops_wakes canary:50 --scenario wakes --mutation snapshot_drops_wakes
1 canary_wakes_index_skips_hear canary:100 --scenario wakes --mutation index_skips_hear
1 canary_wakes_unbounded canary:100 --scenario wakes --mutation unbounded
8 imports pass --scenario imports
2 imports_twice pass --scenario imports --twice
1 canary_imports_held_skips_commit canary:100 --scenario imports --mutation held_skips_commit
1 canary_imports_reimport_advances canary:100 --scenario imports --mutation reimport_advances
1 canary_imports_export_skips_commit canary:100 --scenario imports --mutation export_skips_commit
1 canary_imports_export_skips_keys canary:100 --scenario imports --mutation export_skips_keys
8 facts pass --scenario facts
2 facts_twice pass --scenario facts --twice
1 canary_facts_undecided_compaction canary:20 --scenario facts --mutation undecided_compaction
1 canary_facts_dispatch_before_intent canary:20 --scenario facts --mutation dispatch_before_intent
1 canary_facts_replays_intent canary:20 --scenario facts --mutation replays_intent
1 canary_facts_stale_snapshot_trusted canary:20 --scenario facts --mutation stale_snapshot_trusted
1 canary_facts_base_ignored canary:20 --scenario facts --mutation base_ignored
1 canary_facts_sealed_relowered canary:20 --scenario facts --mutation sealed_relowered
1 canary_facts_facts_resident canary:20 --scenario facts --mutation facts_resident
1 canary_facts_blobs_resident canary:20 --scenario facts --mutation blobs_resident
8 backend_facts pass --scenario backend_facts
2 backend_facts_twice pass --scenario backend_facts --twice
1 canary_backend_facts_result_without_intent canary:20 --scenario backend_facts --mutation result_without_intent
1 canary_backend_facts_media_unrendered canary:20 --scenario backend_facts --mutation media_unrendered
1 canary_backend_facts_undecided_notice canary:20 --scenario backend_facts --mutation undecided_notice
1 canary_backend_facts_abandoned_commit_poisons canary:20 --scenario backend_facts --mutation abandoned_commit_poisons
1 canary_backend_facts_demand_waits_any_prefetch canary:20 --scenario backend_facts --mutation demand_waits_any_prefetch
1 canary_backend_facts_persistence_on_admission_lane canary:20 --scenario backend_facts --mutation persistence_on_admission_lane
1 canary_backend_facts_persist_ignores_revision canary:20 --scenario backend_facts --mutation persist_ignores_revision
1 canary_backend_facts_abandoned_persist_keeps_revision canary:20 --scenario backend_facts --mutation abandoned_persist_keeps_revision
1 canary_backend_facts_lend_blocks_owner canary:20 --scenario backend_facts --mutation lend_blocks_owner
1 canary_backend_facts_steer_waits_conversation canary:20 --scenario backend_facts --mutation steer_waits_conversation
8 secrets pass --scenario secrets
2 secrets_twice pass --scenario secrets --twice
1 canary_secrets_paths_ignored canary:20 --scenario secrets --mutation paths_ignored
1 canary_secrets_debug_shows_value canary:20 --scenario secrets --mutation debug_shows_value
1 canary_secrets_ambiguity_ignored canary:100 --scenario secrets --mutation ambiguity_ignored
8 live pass --scenario live
2 live_twice pass --scenario live --twice
1 canary_live_settle_drops_deletes canary:20 --scenario live --mutation settle_drops_deletes
1 canary_live_settle_keeps_start canary:20 --scenario live --mutation settle_keeps_start
1 canary_live_resume_skips_first canary:50 --scenario live --mutation resume_skips_first
1 canary_live_floor_ignored canary:20 --scenario live --mutation floor_ignored
1 canary_live_rebase_skipped canary:20 --scenario live --mutation rebase_skipped
1 canary_live_snapshot_skips_row canary:20 --scenario live --mutation snapshot_skips_row
1 canary_live_snapshot_at_head canary:20 --scenario live --mutation snapshot_at_head
1 canary_live_outline_leaks canary:20 --scenario live --mutation outline_leaks
1 canary_live_encode_every_frame canary:20 --scenario live --mutation encode_every_frame
1 canary_live_rebase_drops_touched canary:100 --scenario live --mutation rebase_drops_touched
1 canary_live_repeats_reapplied canary:20 --scenario live --mutation repeats_reapplied
1 canary_live_ack_before_commit canary:20 --scenario live --mutation ack_before_commit
1 canary_live_drop_on_ack canary:20 --scenario live --mutation drop_on_ack
1 canary_live_unknown_refuses canary:20 --scenario live --mutation unknown_refuses
1 canary_live_ack_early canary:20 --scenario live --mutation ack_early
1 canary_live_instance_per_socket canary:20 --scenario live --mutation instance_per_socket
1 canary_live_overload_keeps_frames canary:20 --scenario live --mutation overload_keeps_frames
1 canary_live_coalesce_drops_frame canary:20 --scenario live --mutation coalesce_drops_frame
1 canary_live_rebuild_skips_first canary:20 --scenario live --mutation rebuild_skips_first
8 controlplane pass --scenario controlplane --ops 2000
2 controlplane_twice pass --scenario controlplane --ops 1500 --twice
1 canary_controlplane_claim_ignores_lease canary:32 --scenario controlplane --ops 300 --mutation claim_ignores_lease
1 canary_controlplane_post_overwrites canary:32 --scenario controlplane --ops 300 --mutation post_overwrites
1 canary_controlplane_next_ignores_rank canary:32 --scenario controlplane --ops 300 --mutation next_ignores_rank
1 canary_controlplane_budget_skips_inflight canary:64 --scenario controlplane --ops 300 --mutation budget_skips_inflight
1 canary_controlplane_wake_dropped canary:32 --scenario controlplane --ops 300 --mutation wake_dropped
1 canary_controlplane_goal_finished_early canary:32 --scenario controlplane --ops 300 --mutation goal_finished_early
1 canary_controlplane_spawn_retry_renames canary:32 --scenario controlplane --ops 300 --mutation spawn_retry_renames
8 partitions pass --scenario partitions
2 partitions_twice pass --scenario partitions --twice
1 canary_partitions_unfenced canary:64 --scenario partitions --mutation unfenced
1 canary_partitions_claim_keeps_epoch canary:64 --scenario partitions --mutation claim_keeps_epoch
1 canary_partitions_waits_for_free canary:64 --scenario partitions --mutation waits_for_free
1 canary_partitions_keeps_moved canary:64 --scenario partitions --mutation keeps_moved
1 canary_partitions_assign_by_index canary:64 --scenario partitions --mutation assign_by_index
1 canary_partitions_replays_in_flight canary:64 --scenario partitions --mutation replays_in_flight
1 canary_partitions_double_claim canary:64 --scenario partitions --mutation double_claim
1 canary_partitions_lost_claim canary:64 --scenario partitions --mutation lost_claim
1 canary_partitions_unbounded_load canary:64 --scenario partitions --mutation unbounded_load
8 lease pass --scenario lease
2 lease_twice pass --scenario lease --twice
1 canary_lease_claim_keeps_epoch canary:64 --scenario lease --mutation claim_keeps_epoch
1 canary_lease_release_drops_cursor canary:400 --scenario lease --mutation release_drops_cursor
1 canary_lease_renew_ignores_epoch canary:400 --scenario lease --mutation renew_ignores_epoch
1 canary_lease_claim_skips_grace canary:64 --scenario lease --mutation claim_skips_grace
1 canary_lease_claim_steals_live canary:64 --scenario lease --mutation claim_steals_live
1 canary_lease_write_skips_head_cas canary:64 --scenario lease --mutation write_skips_head_cas
1 canary_lease_commit_locks_lease canary:64 --scenario lease --mutation commit_locks_lease
4 held_await pass --scenario held_await
1 held_await_twice pass --scenario held_await --twice
1 canary_held_await_held_across_await canary:64 --scenario held_await --mutation held_across_await
1 canary_held_await_waits_unbounded canary:64 --scenario held_await --mutation waits_unbounded
1 canary_held_await_mutex_semaphore canary:64 --scenario held_await --mutation mutex_semaphore
4 durable pass --scenario durable
1 durable_twice pass --scenario durable --twice
1 canary_durable_write_skips_fence canary:64 --scenario durable --mutation write_skips_fence
1 canary_durable_claim_steals_live canary:16 --scenario durable --mutation claim_steals_live
1 canary_durable_claim_skips_grace canary:16 --scenario durable --mutation claim_skips_grace
1 canary_durable_purge_skips_grace canary:2000 --scenario durable --mutation purge_skips_grace
1 canary_durable_commit_skips_revive canary:400 --scenario durable --mutation commit_skips_revive
1 canary_durable_collect_fences_owner canary:16 --scenario durable --mutation collect_fences_owner
1 canary_durable_purge_unbatched canary:16 --scenario durable --mutation purge_unbatched
1 canary_durable_purge_skips_deadline canary:64 --scenario durable --mutation purge_skips_deadline
1 canary_durable_purge_finishes_early canary:16 --scenario durable --mutation purge_finishes_early
4 scale pass --scenario scale
1 scale_twice pass --scenario scale --twice
1 canary_scale_global_lock canary:16 --scenario scale --mutation global_lock
1 canary_scale_row_per_record canary:16 --scenario scale --mutation row_per_record
1 canary_scale_one_node canary:16 --scenario scale --mutation one_node
1 canary_scale_double_claim canary:16 --scenario scale --mutation double_claim
1 scale_skewed pass --scenario scale --load skewed
1 scale_skewed_twice pass --scenario scale --load skewed --twice
1 canary_scale_ignore_hot canary:16 --scenario scale --load skewed --mutation ignore_hot
'
default_known='
'
discovered_weight=8
discovered_twice_weight=2

usage() {
  echo "usage: cfo.sh run | supervise | install | upload | triage | summary [HOURS] | failures [HOURS]" >&2
  exit 2
}

now() { date +%s; }

json_text() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/\t/\\t/g' | tr -d '\r' | tr '\n' ' '
}

record() {
  printf '%s\n' "$1" >>"$results"
}

random_seed() {
  local raw
  raw=$(od -An -N8 -tu8 /dev/urandom | tr -d ' ')
  echo $(( raw & seed_mask ))
}

fetch_and_build() {
  mkdir -p "$root/bins"
  if [ ! -d "$root/nanocode/.git" ]; then
    git clone -q "$remote" "$root/nanocode" || return 1
  fi
  git -C "$root/nanocode" fetch -q origin "$branch" || return 1
  local sha
  sha=$(git -C "$root/nanocode" rev-parse FETCH_HEAD) || return 1
  if [ -x "$root/bins/$sha/nanocode-sim" ]; then
    echo "$sha"
    return 0
  fi
  git -C "$root/nanocode" checkout -q --detach "$sha" || return 1
  local started log
  started=$(now)
  log=$root/build.log
  if ! (cd "$root/nanocode" && CARGO_TARGET_DIR=$root/target CARGO_BUILD_JOBS=$build_jobs NANOCODE_SHARE_GENERICS=lib \
    taskset -c "$build_cpus" cargo build --locked --release -p nanocode-sim) >"$log" 2>&1; then
    record "{\"time\":$started,\"commit\":\"$sha\",\"scenario\":\"build\",\"ok\":false,\"wall_secs\":$(( $(now) - started )),\"message\":\"$(json_text "$(tail -20 "$log")")\"}"
    return 1
  fi
  mkdir -p "$root/bins/$sha"
  cp "$root/target/release/nanocode-sim" "$root/bins/$sha/nanocode-sim"
  housekeep
  record "{\"time\":$started,\"commit\":\"$sha\",\"scenario\":\"build\",\"ok\":true,\"wall_secs\":$(( $(now) - started ))}"
  echo "$sha"
}

load_table() {
  local bin=$1 table scenarios scenario
  if [ -f "$root/table" ]; then
    table=$(cat "$root/table")
  else
    table=$default_table
  fi
  scenarios=$("$bin" 2>&1 | grep -o -- '--scenario [a-z_|]*' | head -1 | sed 's/--scenario //')
  for scenario in $(echo "$scenarios" | tr '|' ' '); do
    if ! echo "$table" | grep -q -- "--scenario $scenario\( \|$\)"; then
      table="$table
$discovered_weight $scenario pass --scenario $scenario
$discovered_twice_weight ${scenario}_twice pass --scenario $scenario --twice"
    fi
  done
  local line mutation
  while read -r line scenario mutation; do
    scenario=${scenario#scenario=}
    mutation=${mutation#mutation=}
    [ "$line" = MUTATION ] && [ -n "$mutation" ] && [ "$mutation" != none ] || continue
    if ! echo "$table" | grep -q -- "--scenario $scenario .*--mutation $mutation\( \|$\)"; then
      table="$table
1 canary_${scenario}_$mutation canary --scenario $scenario --mutation $mutation"
    fi
  done < <("$bin" --mutations 2>/dev/null)
  names=() weights=() kinds=() flags=()
  while read -r weight name kind rest; do
    [ -z "$weight" ] && continue
    case $weight in \#*) continue ;; esac
    scenario=$(echo "$rest" | sed -n 's/.*--scenario \([a-z_]*\).*/\1/p')
    if [ -n "$scenario" ] && ! echo "|$scenarios|" | grep -q "|$scenario|"; then
      continue
    fi
    names+=("$name")
    weights+=("$weight")
    kinds+=("$kind")
    flags+=("$rest")
  done <<<"$table"
}

pick() {
  local best=-1 best_cost=0 at cost
  for at in "${!names[@]}"; do
    cost=$(( (spent[at] * 1000) / weights[at] ))
    if [ "$best" -lt 0 ] || [ "$cost" -lt "$best_cost" ]; then
      best=$at
      best_cost=$cost
    fi
  done
  echo "$best"
}

known_failure() {
  local name=$1 line=$2 known prefix pattern
  if [ -f "$root/known" ]; then
    known=$(cat "$root/known")
  else
    known=$default_known
  fi
  while read -r prefix pattern; do
    [ -z "$prefix" ] && continue
    case $prefix in \#*) continue ;; esac
    case $name in "$prefix"*) ;; *) continue ;; esac
    case $line in *"$pattern"*) return 0 ;; esac
  done <<<"$known"
  return 1
}

run_job() {
  local at=$1 sha=$2 bin=$3 seed=$4 count=$5
  local name=${names[$at]} kind=${kinds[$at]} flags=${flags[$at]}
  local end=$(( seed + count )) out started status wall ok passed failed message timed_out caught=0 floor=1
  out=$(mktemp "$root/tmp/job.XXXXXX")
  started=$(now)
  timeout --verbose --signal=KILL "$job_secs_max" taskset -c "$fuzz_cpus" "$bin" --seeds "$seed..$end" $flags >"$out" 2>&1
  status=$?
  wall=$(( $(now) - started ))
  timed_out=false
  if grep -q '^timeout: sending signal KILL to command' "$out"; then
    timed_out=true
  else
    case $status in
      129 | 130 | 137 | 143)
        echo "$name seed $seed: killed from outside (exit $status: a service restart or out of memory), not recorded" >&2
        rm -f "$out"
        echo "$wall skipped"
        return 0
        ;;
    esac
  fi
  passed=$(grep '^RESULT' "$out" | sed -n 's/.* passed=\([0-9]*\).*/\1/p' | tail -1)
  failed=$(grep '^RESULT' "$out" | sed -n 's/.* failed=\([0-9]*\).*/\1/p' | tail -1)
  passed=${passed:-0}
  failed=${failed:-0}
  ok=false
  message=""
  case $kind in
    pass) [ "$status" -eq 0 ] && ok=true ;;
    canary*)
      caught=$(sed -n 's/^MUTATION .* caught by \([0-9]*\) seeds.*/\1/p' "$out" | tail -1)
      floor=1
      case $kind in canary:*:*) floor=${kind##*:} ;; esac
      [ "$status" -eq 0 ] && [ "${caught:-0}" -ge "$floor" ] && ok=true
      ;;
  esac
  if [ "$timed_out" = true ]; then
    message="killed: timeout after ${job_secs_max}s"
  elif [ "$ok" = false ] && [ "${caught:-0}" -gt 0 ]; then
    message="MUTATION caught by $caught seeds, below the floor of $floor"
  elif [ "$ok" = false ]; then
    message=$(grep -m1 '^FAIL\|^MUTATION\|panicked\|^error' "$out")
    [ -z "$message" ] && message="exit $status: $(tail -3 "$out")"
  fi
  local replay="$bin --seeds $seed..$end $flags" known=false line log_name=""
  [ "$ok" = false ] && log_name=$started-$name-$seed.log
  if [ "$ok" = false ] && [ "$kind" = pass ] && [ "$failed" -gt 0 ] && grep -q '^FAIL' "$out"; then
    known=true
    while read -r line; do
      known_failure "$name" "$line" || { known=false; break; }
    done < <(grep '^FAIL' "$out")
  fi
  record "{\"time\":$started,\"commit\":\"$sha\",\"scenario\":\"$name\",\"kind\":\"$kind\",\"seed\":$seed,\"count\":$count,\"ok\":$ok,\"known\":$known,\"exit\":$status,\"timed_out\":$timed_out,\"passed\":$passed,\"failed\":$failed,\"wall_secs\":$wall,\"log\":\"$log_name\",\"replay\":\"$(json_text "$replay")\",\"message\":\"$(json_text "$message")\"}"
  if [ "$kind" = pass ]; then
    grep '^FAIL' "$out" | head -32 | while read -r line; do
      local failing command seen=false
      known_failure "$name" "$line" && seen=true
      failing=$(echo "$line" | sed -n 's/^FAIL seed=\([0-9]*\).*/\1/p')
      command=$(echo "$line" | sed -n 's/.*:: replay: nanocode-sim//p')
      record "{\"time\":$started,\"commit\":\"$sha\",\"scenario\":\"$name\",\"kind\":\"seed\",\"seed\":${failing:-0},\"count\":1,\"ok\":false,\"known\":$seen,\"log\":\"$log_name\",\"replay\":\"$(json_text "$bin$command")\",\"message\":\"$(json_text "$line")\"}"
    done
  fi
  if [ "$ok" = false ]; then
    mkdir -p "$logs"
    cp "$out" "$logs/$started-$name-$seed.log"
  fi
  rm -f "$out"
  echo "$wall"
}

refill() {
  local at
  for at in "${!names[@]}"; do
    spent[at]=${spent[at]:-0}
    batch[at]=$(cat "$root/state/${names[$at]}.batch" 2>/dev/null || echo "$batch_first")
  done
}

run() {
  mkdir -p "$root/tmp" "$root/state"
  local started sha bin last_refresh deadline building="" uploading="" at
  started=$(now)
  deadline=$(( started + budget_secs ))
  sha=$(fetch_and_build) || sha=$(ls -t "$root/bins" 2>/dev/null | head -1)
  [ -z "$sha" ] && { echo "no nanocode-sim binary built" >&2; return 1; }
  bin=$root/bins/$sha/nanocode-sim
  spent=() batch=()
  load_table "$bin"
  refill
  declare -A running=()
  last_refresh=$started
  while :; do
    local t
    t=$(now)
    if [ -z "$building" ] && [ $(( t - last_refresh )) -ge "$refresh_secs" ] && [ "$t" -lt "$deadline" ]; then
      last_refresh=$t
      ( fetch_and_build >"$root/tmp/build.sha" ) &
      building=$!
      if [ -z "$uploading" ]; then
        ( upload; triage ) >"$root/tmp/upload.log" 2>&1 &
        uploading=$!
      fi
    fi
    while [ "${#running[@]}" -lt "$workers" ] && [ "$(now)" -lt "$deadline" ]; do
      at=$(pick)
      local count=${batch[$at]}
      case ${kinds[$at]} in
        canary) count=$canary_seeds ;;
        canary:*) count=${kinds[$at]#canary:}; count=${count%%:*} ;;
      esac
      local seed
      seed=$(random_seed)
      spent[at]=$(( spent[at] + job_secs_target ))
      ( wall=$(run_job "$at" "$sha" "$bin" "$seed" "$count"); echo "$at $count $wall" >"$root/tmp/done.$BASHPID" ) &
      running[$!]=$at
    done
    [ "${#running[@]}" -eq 0 ] && [ -z "$building" ] && [ -z "$uploading" ] && break
    local finished=""
    wait -n -p finished
    if [ -n "$uploading" ] && [ "$finished" = "$uploading" ]; then
      uploading=""
      continue
    fi
    if [ -n "$building" ] && [ "$finished" = "$building" ]; then
      local next
      next=$(cat "$root/tmp/build.sha" 2>/dev/null)
      if [ -n "$next" ] && [ "$next" != "$sha" ] && [ -x "$root/bins/$next/nanocode-sim" ]; then
        sha=$next
        bin=$root/bins/$sha/nanocode-sim
        load_table "$bin"
        refill
      fi
      building=""
      continue
    fi
    if [ -n "$finished" ] && [ -n "${running[$finished]+x}" ]; then
      unset "running[$finished]"
      local done_file=$root/tmp/done.$finished
      if [ -f "$done_file" ]; then
        local job_at job_count job_wall job_skipped scaled
        read -r job_at job_count job_wall job_skipped <"$done_file"
        rm -f "$done_file"
        spent[job_at]=$(( spent[job_at] - job_secs_target + job_wall ))
        [ -n "$job_skipped" ] && continue
        scaled=$(( job_count * job_secs_target / (job_wall > 0 ? job_wall : 1) ))
        [ "$scaled" -gt $(( job_count * 2 )) ] && scaled=$(( job_count * 2 ))
        [ "$scaled" -lt "$seeds_min" ] && scaled=$seeds_min
        [ "$scaled" -gt "$seeds_max" ] && scaled=$seeds_max
        batch[job_at]=$scaled
        echo "$scaled" >"$root/state/${names[$job_at]}.batch"
      fi
    fi
  done
}

supervise() {
  mkdir -p "$root"
  trap 'kill 0' EXIT INT TERM
  while :; do
    if [ -f "$root/nanocode/bin/nanocode-sim/cfo.sh" ]; then
      cp "$root/nanocode/bin/nanocode-sim/cfo.sh" "$root/cfo.run.sh"
    else
      cp "$here/cfo.sh" "$root/cfo.run.sh"
    fi
    bash "$root/cfo.run.sh" run || sleep 10
  done
}

install() {
  mkdir -p "$HOME/.config/systemd/user" "$root"
  cp "$here/cfo.sh" "$root/cfo.sh"
  cat >"$HOME/.config/systemd/user/nanocode-cfo.service" <<EOF
[Unit]
Description=nanocode-sim continuous fuzzing of nanocode $branch

[Service]
Environment=NANOCODE_CFO_ROOT=$root
Environment=PATH=$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin
ExecStart=/bin/bash $root/cfo.sh supervise
Restart=always
RestartSec=10
MemoryMax=$memory_max
KillMode=control-group
Nice=10
CPUAffinity=$build_cpus $fuzz_cpus

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload
  systemctl --user enable nanocode-cfo.service
  systemctl --user restart nanocode-cfo.service
}

field_awk='
function field(name,   pattern, value) {
  pattern = "\"" name "\":(\"([^\"\\\\]|\\\\.)*\"|[^,}]*)"
  if (!match($0, pattern)) return ""
  value = substr($0, RSTART + length(name) + 3, RLENGTH - length(name) - 3)
  gsub(/^"|"$/, "", value)
  return value
}'

merge_awk='
function see(commit, time) {
  if (!(commit in seen) || time < seen[commit]) seen[commit] = time
}
function add_failure(key, seed, time, replay, message, known, logname, more,   n) {
  n = kept[key] + 0
  if (n < failures_kept) {
    f_seed[key, n] = seed
    f_time[key, n] = time
    f_replay[key, n] = replay
    f_message[key, n] = message
    f_known[key, n] = known
    f_log[key, n] = logname
    f_more[key, n] = more
    kept[key] = n + 1
  } else {
    f_more[key, failures_kept - 1] += 1 + more
  }
}
function stats(key, kind, runs, count, failed, bad, known, killed, first, last) {
  kinds[key] = kind
  r_runs[key] += runs
  r_seeds[key] += count
  r_failed[key] += failed
  r_bad[key] += bad
  r_known[key] += known
  r_killed[key] += killed
  if (!(key in r_first) || first < r_first[key]) r_first[key] = first
  if (last > r_last[key]) r_last[key] = last
}
{
  type = field("type")
  commit = field("commit")
  if (type == "machine") {
    if (field("host") != host) machines[field("host")] = field("time")
    next
  }
  if (commit == "") next
  time = field("time") + 0
  if (time > 0) see(commit, time)
  if (type == "commit") {
    if (field("build") != "") build[commit] = field("build")
    if (field("build_message") != "") build_message[commit] = field("build_message")
    next
  }
  if (type == "fuzzer") {
    stats(commit SUBSEP field("fuzzer"), field("kind"), field("runs"), field("seeds"), field("failed_seeds"), field("failed_runs"), field("known_runs"), field("killed_runs"), field("first") + 0, field("last") + 0)
    next
  }
  if (type == "failure") {
    key = commit SUBSEP field("fuzzer")
    add_failure(key, field("seed"), time, field("replay"), field("message"), field("known"), field("log"), field("more") + 0)
    if (field("reproduced") != "") triage[key, field("seed")] = "\"reproduced\":" field("reproduced") ",\"first_bad\":\"" field("first_bad") "\",\"last_good\":\"" field("last_good") "\""
    next
  }
  scenario = field("scenario")
  kind = field("kind")
  if (scenario == "build") {
    build[commit] = field("ok") == "true" ? "ok" : "failed"
    if (field("ok") != "true") build_message[commit] = field("message")
    next
  }
  key = commit SUBSEP scenario
  if (kind == "triage") {
    triage[key, field("seed")] = "\"reproduced\":" field("reproduced") ",\"first_bad\":\"" field("first_bad") "\",\"last_good\":\"" field("last_good") "\""
    next
  }
  if (kind == "seed") {
    add_failure(key, field("seed"), time, field("replay"), field("message"), field("known"), field("log"), 0)
    next
  }
  ok = field("ok") == "true"
  known = field("known") == "true"
  stats(key, kind ~ /^canary/ ? "canary" : "pass", 1, field("count"), field("failed"), !ok && !known, !ok && known, field("exit") == "137", time, time)
  if (!ok && (kind ~ /^canary/ || field("failed") + 0 == 0 || field("message") !~ /^FAIL/))
    add_failure(key, field("seed"), time, field("replay"), field("message"), field("known"), field("log"), 0)
}
END {
  n = 0
  for (commit in seen) order[++n] = seen[commit] "\t" commit
  asort(order, order, "@val_str_desc")
  for (i = 1; i <= n && i <= commits_kept; i++) {
    split(order[i], parts, "\t")
    keep[parts[2]] = 1
  }
  printf "{\"type\":\"machine\",\"host\":\"%s\",\"time\":%d}\n", host, now
  for (machine in machines) printf "{\"type\":\"machine\",\"host\":\"%s\",\"time\":%d}\n", machine, machines[machine]
  for (commit in keep) printf "{\"type\":\"commit\",\"commit\":\"%s\",\"time\":%d,\"build\":\"%s\",\"build_message\":\"%s\"}\n", commit, seen[commit], build[commit], build_message[commit]
  for (key in kinds) {
    split(key, parts, SUBSEP)
    if (!(parts[1] in keep)) continue
    printf "{\"type\":\"fuzzer\",\"commit\":\"%s\",\"fuzzer\":\"%s\",\"kind\":\"%s\",\"runs\":%d,\"seeds\":%d,\"failed_seeds\":%d,\"failed_runs\":%d,\"known_runs\":%d,\"killed_runs\":%d,\"first\":%d,\"last\":%d}\n", parts[1], parts[2], kinds[key], r_runs[key], r_seeds[key], r_failed[key], r_bad[key], r_known[key], r_killed[key], r_first[key], r_last[key]
  }
  for (key in kept) {
    split(key, parts, SUBSEP)
    if (!(parts[1] in keep)) continue
    for (i = 0; i < kept[key]; i++) {
      extra = ((key, f_seed[key, i]) in triage) ? "," triage[key, f_seed[key, i]] : ""
      printf "{\"type\":\"failure\",\"commit\":\"%s\",\"fuzzer\":\"%s\",\"seed\":%s,\"time\":%d,\"known\":%s,\"more\":%d,\"log\":\"%s\",\"replay\":\"%s\",\"message\":\"%s\"%s}\n", parts[1], parts[2], f_seed[key, i] == "" ? 0 : f_seed[key, i], f_time[key, i], f_known[key, i] == "true" ? "true" : "false", f_more[key, i], f_log[key, i], f_replay[key, i], f_message[key, i], extra
      if (f_log[key, i] != "") print f_log[key, i] >keep_logs
    }
  }
}'

readme_awk='
function unescape(text) {
  gsub(/\\"/, "\"", text)
  gsub(/\\\\/, "\\", text)
  return text
}
function cell(text) {
  text = unescape(text)
  gsub(/\|/, "\\|", text)
  gsub(/`/, "'"'"'", text)
  if (length(text) > message_chars) text = substr(text, 1, message_chars) "…"
  return text
}
function command(text) {
  text = unescape(text)
  sub(/^[^ ]*nanocode-sim/, "nanocode-sim", text)
  return text
}
function day(time) {
  return strftime("%Y-%m-%d %H:%M", time, 1)
}
{
  type = field("type")
  if (type == "machine") machines[field("host")] = field("time")
  else if (type == "commit") {
    commit = field("commit")
    seen[commit] = field("time") + 0
    build[commit] = field("build")
    build_message[commit] = field("build_message")
  } else if (type == "fuzzer") {
    commit = field("commit")
    key = commit SUBSEP field("fuzzer")
    kinds[key] = field("kind")
    runs[key] = field("runs")
    seeds[key] = field("seeds")
    failed[key] = field("failed_seeds")
    bad[key] = field("failed_runs")
    if (kinds[key] == "pass") {
      c_seeds[commit] += field("seeds")
      c_failed[commit] += field("failed_seeds")
      c_runs[commit] += field("runs")
      if (field("failed_runs") + 0 > 0) c_failing[commit] = c_failing[commit] " " field("fuzzer")
    } else {
      c_canaries[commit]++
      if (field("failed_runs") + 0 > 0) c_missed[commit] = c_missed[commit] " " field("fuzzer")
    }
  } else if (type == "failure" && field("known") != "true") {
    commit = field("commit")
    n = nfail[commit]++
    fl_fuzzer[commit, n] = field("fuzzer")
    fl_seed[commit, n] = field("seed")
    fl_more[commit, n] = field("more")
    fl_log[commit, n] = field("log")
    fl_replay[commit, n] = field("replay")
    fl_message[commit, n] = field("message")
    fl_triage[commit, n] = field("reproduced") == "" ? "" : field("reproduced") == "true" ? "reproduces alone; first bad " substr(field("first_bad"), 1, 9) (field("last_good") == "" ? " or older" : ", last good " substr(field("last_good"), 1, 9)) : "does not reproduce alone (load or nondeterminism)"
  }
}
END {
  n = 0
  for (commit in seen) order[++n] = seen[commit] "\t" commit
  asort(order, order, "@val_str_desc")
  print "# nanocode CFO results"
  print ""
  print "Continuous fuzzing of [nanocodexyz/nanocode](https://github.com/nanocodexyz/nanocode) master by `bin/nanocode-sim/cfo.sh`, modeled on tempoxyz/zempo-cfo-db. Updated " day(now) " UTC. Records: `fuzzing/data.jsonl`; failure logs: `fuzzing/logs/`. A failing seed on master is fixed, or its change reverted, at once."
  print ""
  print "## Machines"
  print ""
  print "| host | last upload (UTC) |"
  print "|---|---|"
  for (machine in machines) print "| " machine " | " day(machines[machine]) " |"
  print ""
  print "## Failing commits"
  print ""
  shown = 0
  for (i = 1; i <= n && shown < failing_shown; i++) {
    split(order[i], parts, "\t")
    commit = parts[2]
    if (c_failing[commit] == "" && build[commit] != "failed" && c_missed[commit] == "") continue
    shown++
    print "### [" substr(commit, 1, 9) "](https://github.com/nanocodexyz/nanocode/commit/" commit ") (first fuzzed " day(seen[commit]) " UTC)"
    print ""
    if (build[commit] == "failed") print "Build failed: `" cell(build_message[commit]) "`\n"
    print "| fuzzer | seed | failure | replay | log | triage |"
    print "|---|---|---|---|---|---|"
    for (j = 0; j < nfail[commit]; j++) {
      more = fl_more[commit, j] + 0 > 0 ? " (+" fl_more[commit, j] " more)" : ""
      log_cell = fl_log[commit, j] == "" ? "" : "[log](fuzzing/logs/" fl_log[commit, j] ")"
      print "| " fl_fuzzer[commit, j] more " | " fl_seed[commit, j] " | " cell(fl_message[commit, j]) " | `" command(fl_replay[commit, j]) "` | " log_cell " | " fl_triage[commit, j] " |"
    }
    print ""
  }
  if (shown == 0) print "None of the newest " n " commits has an unknown failure.\n"
  newest = ""
  for (i = 1; i <= n; i++) {
    split(order[i], parts, "\t")
    if (c_canaries[parts[2]] > 0) { newest = parts[2]; break }
  }
  print "## Canaries"
  print ""
  if (newest != "") {
    print "On " substr(newest, 1, 9) ". A canary runs a mutation the simulator must catch. A canary that caught nothing means its check stopped working."
    print ""
    print "| canary | runs | seeds | caught seeds | catch rate | missed runs |"
    print "|---|---|---|---|---|---|"
    m = 0
    for (key in kinds) {
      split(key, parts, SUBSEP)
      if (parts[1] != newest || kinds[key] != "canary") continue
      rows[++m] = parts[2]
    }
    asort(rows)
    for (j = 1; j <= m; j++) {
      key = newest SUBSEP rows[j]
      printf "| %s | %d | %d | %d | %.1f%% | %d |\n", rows[j], runs[key], seeds[key], failed[key], (seeds[key] > 0 ? 100 * failed[key] / seeds[key] : 0), bad[key]
    }
    print ""
  }
  print "## Seeds per commit"
  print ""
  print "| commit | first fuzzed (UTC) | build | runs | seeds | failed seeds | failing fuzzers | missed canaries |"
  print "|---|---|---|---|---|---|---|---|"
  for (i = 1; i <= n; i++) {
    split(order[i], parts, "\t")
    commit = parts[2]
    print "| " substr(commit, 1, 9) " | " day(seen[commit]) " | " build[commit] " | " c_runs[commit] + 0 " | " c_seeds[commit] + 0 " | " c_failed[commit] + 0 " | " c_failing[commit] " | " c_missed[commit] " |"
  }
}'

upload() {
  local db=$root/db offset size lines consumed try name file
  mkdir -p "$root/state" "$root/tmp"
  if [ ! -d "$db/.git" ]; then
    rm -rf "$db"
    git clone -q "$db_remote" "$db" || return 1
  fi
  size=$(stat -c %s "$results" 2>/dev/null || echo 0)
  offset=$(cat "$root/state/uploaded" 2>/dev/null || echo $(( size > upload_first_bytes ? size - upload_first_bytes : 0 )))
  [ "$size" -lt "$offset" ] && offset=0
  tail -c +$(( offset + 1 )) "$results" | head -c $(( size - offset )) >"$root/tmp/upload.raw"
  lines=$(wc -l <"$root/tmp/upload.raw")
  head -n "$lines" "$root/tmp/upload.raw" >"$root/tmp/upload.jsonl"
  consumed=$(wc -c <"$root/tmp/upload.jsonl")
  if [ "$offset" -gt 0 ] && [ "$(tail -c +"$offset" "$results" | head -c 1)" != $'\n' ]; then
    sed -i 1d "$root/tmp/upload.jsonl"
  fi
  rm -f "$root/tmp/upload.raw"
  for ((try = 0; try < upload_tries; try++)); do
    git -C "$db" fetch -q origin 2>/dev/null
    if git -C "$db" rev-parse -q --verify origin/data >/dev/null; then
      git -C "$db" checkout -q -B data origin/data || return 1
      git -C "$db" reset -q --hard origin/data || return 1
    else
      git -C "$db" checkout -q --orphan data 2>/dev/null
      git -C "$db" rm -rfq . 2>/dev/null
    fi
    git -C "$db" clean -fdxq
    mkdir -p "$db/fuzzing/logs"
    touch "$db/fuzzing/data.jsonl"
    : >"$root/tmp/keep_logs"
    cat "$db/fuzzing/data.jsonl" "$root/tmp/upload.jsonl" | gawk -v host="$host" -v now="$(now)" \
      -v commits_kept="$commits_kept" -v failures_kept="$failures_kept" -v keep_logs="$root/tmp/keep_logs" \
      "$field_awk$merge_awk" >"$root/tmp/data.jsonl" || return 1
    mv "$root/tmp/data.jsonl" "$db/fuzzing/data.jsonl"
    while read -r name; do
      file=$db/fuzzing/logs/$name
      [ -f "$file" ] || [ ! -f "$logs/$name" ] || tail -c "$log_bytes_max" "$logs/$name" >"$file"
    done <"$root/tmp/keep_logs"
    for file in "$db"/fuzzing/logs/*; do
      [ -f "$file" ] || continue
      grep -qxF "$(basename "$file")" "$root/tmp/keep_logs" || rm -f "$file"
    done
    gawk -v now="$(now)" -v failing_shown="$failing_shown" -v message_chars="$message_chars" \
      "$field_awk$readme_awk" "$db/fuzzing/data.jsonl" >"$db/README.md" || return 1
    git -C "$db" add -A
    if git -C "$db" diff --cached --quiet; then
      echo $(( offset + consumed )) >"$root/state/uploaded"
      return 0
    fi
    git -C "$db" -c user.name="cfo-$host" -c user.email="cfo-$host@users.noreply.github.com" commit -q -m "🌱 $host" || return 1
    if git -C "$db" push -q origin data; then
      echo $(( offset + consumed )) >"$root/state/uploaded"
      return 0
    fi
  done
  return 1
}

fails() {
  local sha=$1 status
  shift
  timeout --signal=KILL "$triage_secs" taskset -c "$build_cpus" "$root/bins/$sha/nanocode-sim" "$@" >/dev/null 2>&1
  status=$?
  [ "$status" -ne 0 ] && [ "$status" -ne 2 ]
}

triage() {
  local picked commit scenario seed rest started reproduced=false first_bad="" last_good="" list bad good mid args
  mkdir -p "$root/state"
  touch "$root/state/triaged"
  picked=$(tail -c "$triage_window_bytes" "$results" | grep '"kind":"seed"' | grep '"known":false' |
    awk "$field_awk"'{ print field("commit"), field("scenario"), field("seed"), field("replay") }' |
    while read -r commit scenario seed bin rest; do
      grep -qxF "$commit $scenario" "$root/state/triaged" || echo "$commit $scenario $seed $rest"
    done | tail -1)
  [ -z "$picked" ] && return 0
  read -r commit scenario seed rest <<<"$picked"
  echo "$commit $scenario" >>"$root/state/triaged"
  [ -x "$root/bins/$commit/nanocode-sim" ] || return 0
  started=$(now)
  read -r -a args <<<"$rest"
  if fails "$commit" "${args[@]}"; then
    reproduced=true
    mapfile -t list < <(git -C "$root/nanocode" rev-list --first-parent "$commit" 2>/dev/null | while read -r sha; do
      [ -x "$root/bins/$sha/nanocode-sim" ] && echo "$sha"
    done)
    bad=0
    good=${#list[@]}
    if [ "$good" -gt 1 ] && ! fails "${list[good - 1]}" "${args[@]}"; then
      good=$(( good - 1 ))
      while [ $(( good - bad )) -gt 1 ]; do
        mid=$(( (bad + good) / 2 ))
        if fails "${list[mid]}" "${args[@]}"; then bad=$mid; else good=$mid; fi
      done
      last_good=${list[good]}
    else
      bad=$(( good - 1 ))
    fi
    first_bad=${list[bad]:-$commit}
  fi
  record "{\"time\":$started,\"commit\":\"$commit\",\"scenario\":\"$scenario\",\"kind\":\"triage\",\"seed\":$seed,\"reproduced\":$reproduced,\"first_bad\":\"$first_bad\",\"last_good\":\"$last_good\",\"wall_secs\":$(( $(now) - started )),\"replay\":\"$(json_text "$rest")\"}"
}

housekeep() {
  local size uploaded
  ls -t "$root/bins" 2>/dev/null | tail -n +$(( commits_kept + 1 )) | while read -r old; do
    rm -rf "${root:?}/bins/$old"
  done
  find "$logs" -name '*.log' -mtime +"$log_days" -delete 2>/dev/null
  size=$(stat -c %s "$results" 2>/dev/null || echo 0)
  uploaded=$(cat "$root/state/uploaded" 2>/dev/null || echo 0)
  if [ "$size" -gt "$results_bytes_max" ] && [ "$uploaded" -ge "$size" ]; then
    mv "$results" "$results.1"
    echo 0 >"$root/state/uploaded"
  fi
}

summary() {
  local since=$(( $(now) - ${1:-24} * 3600 ))
  awk -v since="$since" "$field_awk"'
    {
      time = field("time") + 0
      if (time < since) next
      kind = field("kind")
      scenario = field("scenario")
      if (kind == "seed" || scenario == "build") next
      runs[scenario]++
      seeds[scenario] += field("count")
      if (field("ok") != "true" && field("known") == "true") known[scenario]++
      else if (field("ok") != "true") bad[scenario]++
      total += field("count")
      if (time < first || first == 0) first = time
      if (time > last) last = time
      commits[field("commit")] = 1
    }
    END {
      span = last - first
      if (span <= 0) span = 1
      printf "%-36s %8s %12s %8s %8s %10s\n", "scenario", "runs", "seeds", "failed", "known", "seeds/h"
      for (s in runs) printf "%-36s %8d %12d %8d %8d %10d\n", s, runs[s], seeds[s], bad[s], known[s], seeds[s] * 3600 / span
      n = 0
      for (c in commits) n++
      printf "total %d seeds over %.2f h, %d per hour, %d commits\n", total, span / 3600, total * 3600 / span, n
    }' "$results"
}

failures() {
  local since=$(( $(now) - ${1:-24} * 3600 ))
  awk -v since="$since" "$field_awk"'
    {
      if (field("time") + 0 < since || field("ok") == "true" || field("scenario") == "build") next
      if (field("kind") == "pass" && field("failed") + 0 > 0) next
      group = field("message")
      sub(/ :: replay:.*/, "", group)
      gsub(/[0-9]+/, "N", group)
      if (field("known") == "true") group = "[known] " group
      key = field("scenario") " | " group
      count[key]++
      if (!(key in example)) example[key] = field("commit") " " field("replay")
    }
    END {
      for (key in count) printf "%6d  %s\n        %s\n", count[key], key, example[key]
    }' "$results"
}

case ${1:-} in
  run) run ;;
  supervise) supervise ;;
  install) install ;;
  upload) upload ;;
  triage) triage ;;
  summary) summary "${2:-24}" ;;
  failures) failures "${2:-24}" ;;
  *) usage ;;
esac
