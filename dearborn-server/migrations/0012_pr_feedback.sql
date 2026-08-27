-- Post-PR review feedback lifecycle storage (epic plan §6.3). The
-- `pr_feedback` table is the source of truth for what the factory has
-- already handled and for every comment/review it posted itself. Identity is
-- DB-tracked (Decision 1): Dearborn posts with the project owner's PAT, so its
-- replies and a human's feedback share one GitHub identity and author-based
-- filtering is impossible.
--
-- `source_kind` is the GitHub entity that owns each row and is the identity
-- basis of the UNIQUE dedup index:
--   'review'          -> a formal PR review (id is the review id)
--   'review_comment'  -> a diff (review) comment (id is the comment id)
--   'issue_comment'   -> a top-level PR comment (id is the comment id)
--   'our_post'        -> a comment/review the factory posted itself (id is
--                        the id GitHub handed back, so it is never reprocessed)
--
-- `state` marks the lifecycle of handled feedback:
--   'handled_reply'   -> replied, thread resolved (question / any-reply case)
--   'in_progress'     -> change-request work has been spawned, not yet landed
--   'addressed'       -> work landed; "Addressed in <commit>" posted
--
-- `epic_id`/`task_id` identify the InReview item the feedback belongs to (one
-- of the two is set); `spawned_task_ids` is the JSON array of tasks created
-- for a change request; `base_sha` is the branch HEAD when work was picked up.
CREATE TABLE pr_feedback (
  id               TEXT    PRIMARY KEY,      -- ulid
  project_id       TEXT    NOT NULL,
  epic_id          TEXT,                     -- one of epic_id/task_id set
  task_id          TEXT,
  pr_number        INTEGER NOT NULL,
  source_kind      TEXT    NOT NULL,         -- 'review'|'review_comment'|'issue_comment'|'our_post'
  github_id        INTEGER NOT NULL,         -- review/comment id (for our_post: the id we created)
  thread_id        TEXT,                     -- GraphQL thread id for inline (NULL otherwise)
  classification   TEXT,                     -- 'question' | 'change_request' (set at triage)
  state            TEXT    NOT NULL,         -- 'handled_reply' | 'in_progress' | 'addressed'
  spawned_task_ids TEXT,                     -- JSON array of task ids created for a change request
  base_sha         TEXT,                     -- branch HEAD when work was picked up
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_pr_feedback_ident
  ON pr_feedback(pr_number, source_kind, github_id);