# Task List: execute-current-project

## Meta
- Created: 2026-02-28 16:02
- Task Directory: .autonomous/execute-current-project
- Total Tasks: 32
- Completed: 0/32 (0%)

## Progress Notes
<!-- Updated after each session -->
- 2026-02-28 Session 1 initialized task breakdown and progress tracking.

## Tasks

### Phase 1: Foundation
- [ ] Task 1: Read `AGENT_PROMPT.md` and extract hard constraints for task selection, locking, evaluation, commit policy, and logging.
- [ ] Task 2: Inspect `current_tasks/` and build an ordered queue of files with `status: open` based on expected impact and effort.
- [ ] Task 3: Define lock identity format `in_progress_by_<agent_name>` for this autonomous run and record chosen agent_name.
- [ ] Task 4: Claim one open task by editing exactly one `current_tasks/*.txt` status to `in_progress_by_<agent_name>` and adding `locked_at` timestamp.
- [ ] Task 5: Start required services (`docker compose up -d`) and verify containers are healthy.
- [ ] Task 6: Start backend (`uvicorn app.main:app --reload --port 8000`) and confirm readiness endpoint succeeds.
- [ ] Task 7: Run baseline `make eval-fast` before code changes and save metrics snapshot for comparison.
- [ ] Task 8: Identify primary bottleneck or failure mode from baseline outputs and map it to measurable target metric.

### Phase 2: Core Implementation
- [ ] Task 9: Implement first scoped code change for claimed task that directly targets the selected metric.
- [ ] Task 10: Add or update focused automated tests covering the new behavior introduced in Task 9.
- [ ] Task 11: Run `make eval-fast` after Task 9 and capture metric deltas versus baseline.
- [ ] Task 12: If no improvement in target metric, revise implementation with a second scoped change and re-run tests.
- [ ] Task 13: Re-run `make eval-fast` after Task 12 and capture new metric deltas.
- [ ] Task 14: If improvement is confirmed, prepare a commit containing only relevant code, tests, and logs for this iteration.
- [ ] Task 15: Record iteration findings and metrics in `agent_logs/<timestamp>.md` using required log format.
- [ ] Task 16: Select next open task from `current_tasks/` and claim it by status update with timestamp.
- [ ] Task 17: Implement first scoped code change for second claimed task targeting its metric.
- [ ] Task 18: Add or update focused tests for Task 17 behavior.
- [ ] Task 19: Run `make eval-fast` for Task 17 and compare with pre-change baseline for that task.
- [ ] Task 20: If regression or no gain is detected, perform one corrective iteration and re-run tests.
- [ ] Task 21: Re-run `make eval-fast` after Task 20 and record final metric outcome.
- [ ] Task 22: Commit second task improvements only when pass rate increases, latency decreases, or cost decreases.
- [ ] Task 23: Log second task work in `agent_logs/<timestamp>.md` with before/after metrics and next steps.

### Phase 3: Integration & Testing
- [ ] Task 24: Execute `make eval-regression` on the latest integrated code state and save summary metrics.
- [ ] Task 25: Execute `make eval-full` when runtime permits, or document explicit blocker and fallback evidence if not feasible.
- [ ] Task 26: Validate citation correctness behavior on representative cases and quantify citation-related metric impact.
- [ ] Task 27: Validate retrieval quality behavior on representative cases and quantify `hits@top5` impact.
- [ ] Task 28: Validate latency and cost behavior on representative workload and quantify p95/cost impact.

### Phase 4: Polish & Documentation
- [ ] Task 29: Update claimed task files in `current_tasks/` with latest status and concise outcome notes.
- [ ] Task 30: Produce consolidated run summary in `agent_logs/<timestamp>.md` covering challenges, findings, changes, and metrics.
- [ ] Task 31: Ensure git history contains only metric-backed improvement commits and no unrelated file changes.
- [ ] Task 32: Prepare handoff notes in `.autonomous/execute-current-project/progress.md` indicating next highest-priority actionable task.
