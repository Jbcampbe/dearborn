---
name: to-tasks
description: Break a plan, spec, or PRD into independently-grabbable tasks using tracer-bullet vertical slices. Use when user wants to convert a plan into tasks, create implementation tickets, or break down work into issues.
---

Plan file: $@

# To Tasks

Break a plan into independently-grabbable tasks using vertical slices (tracer bullets).

## Process

### 1. Gather context

Use the PRD that the user will specify

### 2. Explore the codebase (optional)

If you have not already explored the codebase, do so to understand the current state of the code.

### 3. Draft vertical slices

Break the plan into **tracer bullet** tasks. Each task is a thin vertical slice that cuts through ALL integration layers end-to-end, NOT a horizontal slice of one layer.

<vertical-slice-rules>
- Each slice delivers a narrow but COMPLETE path through every layer (schema, API, UI, tests)
- A completed slice is demoable or verifiable on its own
- Prefer many thin slices over few thick ones
</vertical-slice-rules>

### 4. Create the tasks using beads

For each approved slice, create a beads task. Use the issue body template below.

Create issues in dependency order (blockers first) so you can properly link the dependencies together.

```
bd create "<task title>" --id=<task id so you can easily link deps together> --description="<A concise description of this vertical slice. Describe the end-to-end behavior, not layer-by-layer implementation.>" --acceptance="<acceptance criteria>" --deps="blocks:<task id for anything that is blocked by this task (if applicable)>"
```
