# Dearborn Scratchpad

Ideas
- Agent that watches observability system
- Scheduled jobs
- secrets and .env file for agent to run locally
- Give planning agents the ability to improve their own skills?
- How to make merging better? Would be ideal if PRs would automatically merge that latest changes from the base branch. Agent handles fixing merge conflicts
- How to handle iteration on the PR
- Atachments for context during planning
- Prototypes/Diagrams during planning
- Tradeoffs/Architecture/Code Design
- Multi-repo support
- Collaboration and feedback on plans

Missing Features
- Better auth model
- Multi-user support
- Global settings
  - Configure which coding agents + api keys, login, etc.
  - Configure skills/plugins/etc.
  - Set default harness/model
- General project-level chat (for questions, general brainstorming, etc.)
- Project settings
    - Override agent harness/models for specific agents
    - Agent system prompt(s) (or additional prompts maybe)
    - Configure implementation pipeline (which steps, how many attempts)
    - Set a home branch (everything will branch off of and PR into this branch)
    - Configure single PR vs. PR per task (PR chain)
- Better DAG editor
- Mobile App
  - TTS and STT for planning
- Agent memory
- Feature toggles
- Observability
- Incident response
