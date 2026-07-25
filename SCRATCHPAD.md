# Dearborn Scratchpad

Ideas
- Create Dearborn task from Slack or the cli?
- Agent that watches observability system
- Scheduled jobs
- secrets and .env file for agent to run locally
- Give planning agents the ability to improve their own skills?
- How to make merging better? Would be ideal if PRs would automatically merge that latest changes from the base branch
- How to handle iteration on the PR

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
- Task implementation
    - Record full session onto each task in realtime. Showing progress through the process
- Only allow title/description edits after epic is completed
- Agent memory
- Feature toggles
- Observability
- Incident response
- Mobile App
  - TTS and STT for planning
