---
description: Run Go test and linter to ensure everything is passing
---
Steps:

flowchart TD
    A[Run tests and linter] --> B{All Passing?}
    B -- Yes --> C[Done]
    B -- No --> D{Error Fixable?}
    D -- Yes --> E[Triage & Fix Failures]
    E --> A
    D -- Unsure --> F[Ask for Confirmation to Skip]
    F --> G{Confirmed Skip?}
    G -- Yes --> C
    G -- No --> E

go test command: `go test ./... -json | gotestfmt -hide all` (this will only show you the errors so you do not have to grep or head the output yourself)
golangci-lint command: `gl`
