---
name: implement-next-task
description: Grab the next available task to be worked on and implement.
---

# Implement Next Task

1. Use the beads cli (`bd`) to grab the next available task and implement it

To list tasks that are ready to be picked up
```
bd ready
```

To claim the task
```
bd update <id> --claim
```

2. Ensure that all tests are passing by running `task test`

3. Create a commit briefly summarizing your changes

4. Run the /reviewer subagent to independently review your work and provide feedback. Address its feedback or defend your reasoning on why not

5. Only after all the above are completed successfully can you close the task
```
bd close <id>
```
