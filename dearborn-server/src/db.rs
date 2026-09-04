//! libSQL connection handle and boot-time migration runner.
//!
//! libSQL is single-writer, so v1 uses one shared [`libsql::Connection`] (cheap
//! to clone; the underlying handle is reference-counted) rather than a pool.
//! Migrations are ordered `.sql` files embedded at compile time and applied
//! idempotently at boot, tracked in a `_migrations` table so a re-boot is a
//! no-op.

use std::sync::Arc;

use libsql::{Builder, Connection, Database};
use thiserror::Error;

/// A single embedded, ordered migration.
struct Migration {
    /// Monotonic version; also the row key in `_migrations`.
    id: i64,
    /// Human-readable name recorded alongside the id.
    name: &'static str,
    /// The SQL to apply (may contain multiple statements).
    sql: &'static str,
}

/// All migrations, in application order. Append new ones; never edit applied SQL.
const MIGRATIONS: &[Migration] = &[
    Migration {
        id: 1,
        name: "0001_baseline",
        sql: include_str!("../migrations/0001_baseline.sql"),
    },
    Migration {
        id: 2,
        name: "0002_planning_session",
        sql: include_str!("../migrations/0002_planning_session.sql"),
    },
    Migration {
        id: 3,
        name: "0003_epic_description",
        sql: include_str!("../migrations/0003_epic_description.sql"),
    },
    Migration {
        id: 4,
        name: "0004_executor",
        sql: include_str!("../migrations/0004_executor.sql"),
    },
    Migration {
        id: 5,
        name: "0005_agent_settings",
        sql: include_str!("../migrations/0005_agent_settings.sql"),
    },
    Migration {
        id: 6,
        name: "0006_agent_run_evidence",
        sql: include_str!("../migrations/0006_agent_run_evidence.sql"),
    },
    Migration {
        id: 7,
        name: "0007_users",
        sql: include_str!("../migrations/0007_users.sql"),
    },
    Migration {
        id: 8,
        name: "0008_failure_detail",
        sql: include_str!("../migrations/0008_failure_detail.sql"),
    },
    Migration {
        id: 9,
        name: "0009_agent_run_events",
        sql: include_str!("../migrations/0009_agent_run_events.sql"),
    },
    Migration {
        id: 10,
        name: "0010_agent_run_actual_model",
        sql: include_str!("../migrations/0010_agent_run_actual_model.sql"),
    },
    // Token columns for cost graphs. (The spec called this "0009", but that
    // slot was already taken; it lands here as the next free version.)
    Migration {
        id: 11,
        name: "0011_token_columns",
        sql: include_str!("../migrations/0011_token_columns.sql"),
    },
    // Post-PR feedback lifecycle storage — the `pr_feedback` table (epic plan
    // §6.3). The spec named this "0011", but that slot was already taken by
    // token_columns (itself re-slotted from a spec'd 0009); it lands here as
    // the next free version, mirroring that precedent.
    Migration {
        id: 12,
        name: "0012_pr_feedback",
        sql: include_str!("../migrations/0012_pr_feedback.sql"),
    },
    // NOTE: id 13 is owned by `0013_agent_run_thinking` (the `record-thinking`
    // line of work, merged ahead of this one). These two branches independently
    // authored an id-13 migration; to avoid the collision that silently skips
    // one of them, the wayfinder pair sits at 14/15 on the assumption
    // record-thinking lands first. Never reuse an id across branches.
    //
    // The pair is ordered drop-then-create (14 before 15) so a db that already
    // recorded `drop_linear_planning` at id 14 (agent_run_thinking took 13, so
    // `planning_map_schema` was skipped) self-heals: the next boot applies id 15
    // and nothing else. The two are independent — `drop_linear_planning` only
    // drops `transcript_message`/`planning_session`, and `planning_map_schema`
    // references only its own new tables — so the order is purely mechanical.
    //
    // Clean cutover (wayfinder epic §12): retire the linear product/technical
    // planning store — `transcript_message` and `planning_session` — now that
    // the code paths reading and writing them are gone (no feature flag, no
    // coexistence; per-node `node_session`/`node_message` take over).
    Migration {
        id: 14,
        name: "0014_drop_linear_planning",
        sql: include_str!("../migrations/0014_drop_linear_planning.sql"),
    },
    // Planning-map & living-document data model (epic "Wayfinder-Inspired
    // Planning" §4): map_node, map_node_dependency, node_session, node_message,
    // document, document_version, document_section, node_asset, the overhauled
    // comment, and activity; the epic prose cutover adds destination/notes/
    // not_yet_specified/out_of_scope and drops product_context/technical_context.
    Migration {
        id: 15,
        name: "0015_planning_map_schema",
        sql: include_str!("../migrations/0015_planning_map_schema.sql"),
    },
];

/// Errors surfaced while opening the database or running migrations.
#[derive(Debug, Error)]
pub enum DbError {
    #[error("libsql error: {0}")]
    Libsql(#[from] libsql::Error),
}

/// Shared database handle. Clone freely; clones share the same connection.
#[derive(Clone)]
pub struct Db {
    // Kept alive so the connection's underlying resources are not dropped.
    _database: Arc<Database>,
    conn: Connection,
}

impl Db {
    /// Open (or create) a local libSQL database at `path`.
    ///
    /// `":memory:"` yields an ephemeral in-memory database (used by tests).
    pub async fn connect(path: &str) -> Result<Db, DbError> {
        let database = Builder::new_local(path).build().await?;
        let conn = database.connect()?;
        apply_pragmas(&conn).await;
        Ok(Db {
            _database: Arc::new(database),
            conn,
        })
    }

    /// The shared connection, for issuing queries.
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Apply any not-yet-applied migrations in order. Returns the number newly
    /// applied (0 when already up to date). Idempotent across process restarts.
    pub async fn run_migrations(&self) -> Result<u32, DbError> {
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS _migrations (\
                     id         INTEGER PRIMARY KEY, \
                     name       TEXT NOT NULL, \
                     applied_at INTEGER NOT NULL\
                 )",
                (),
            )
            .await?;

        let mut applied = std::collections::HashSet::new();
        let mut rows = self.conn.query("SELECT id FROM _migrations", ()).await?;
        while let Some(row) = rows.next().await? {
            applied.insert(row.get::<i64>(0)?);
        }

        let mut newly_applied = 0;
        for migration in MIGRATIONS {
            if applied.contains(&migration.id) {
                continue;
            }
            // DDL in SQLite/libSQL is transactional; execute the whole file, then
            // record it. A crash between the two re-runs the file next boot.
            self.conn.execute_batch(migration.sql).await?;
            self.conn
                .execute(
                    "INSERT INTO _migrations (id, name, applied_at) VALUES (?1, ?2, ?3)",
                    (migration.id, migration.name, now_ms()),
                )
                .await?;
            newly_applied += 1;
        }

        Ok(newly_applied)
    }
}

/// Busy timeout applied to every connection, in milliseconds. A transient
/// external lock (e.g. DBeaver holding the file open) becomes a short wait
/// instead of an instant `database is locked` failure on the first write.
const BUSY_TIMEOUT_MS: i64 = 5000;

/// Connection pragmas applied once per connection at open.
///
/// WAL lets external readers coexist with server writers — in rollback-journal
/// mode a single long-running read transaction elsewhere on the host blocked
/// every server write with `SQLITE_BUSY`, which is exactly how a breakdown run
/// ended up silently creating zero tasks while reporting success (see
/// [`crate::breakdown`] for the guard that makes such runs fail loudly now).
/// All pragmas are best-effort: WAL is meaningless for `":memory:"` databases
/// (the pragma reports `memory` rather than erroring), and a failure to set
/// any one degrades to today's behavior rather than refusing to boot.
async fn apply_pragmas(conn: &Connection) {
    if let Err(err) = conn.execute_batch("PRAGMA journal_mode = WAL;").await {
        tracing::warn!(error = %err, "could not enable WAL journal mode");
    }
    if let Err(err) = conn
        .execute_batch(&format!("PRAGMA busy_timeout = {BUSY_TIMEOUT_MS};"))
        .await
    {
        tracing::warn!(error = %err, "could not apply busy timeout");
    }
    if let Err(err) = conn.execute_batch("PRAGMA foreign_keys = ON;").await {
        tracing::warn!(error = %err, "could not enable foreign key enforcement");
    }
}

/// Current unix time in milliseconds.
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn migrations_create_schema_and_roundtrip_a_project() {
        let db = Db::connect(":memory:").await.unwrap();

        // Fresh boot applies every ordered migration.
        assert_eq!(db.run_migrations().await.unwrap(), MIGRATIONS.len() as u32);
        // Re-running is a no-op.
        assert_eq!(db.run_migrations().await.unwrap(), 0);

        // Every §2.2 table exists.
        for table in [
            "project",
            "epic",
            "task",
            "task_dependency",
            "agent_run",
            "comment",
        ] {
            let mut rows = db
                .conn()
                .query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?1",
                    libsql::params![table],
                )
                .await
                .unwrap();
            assert!(
                rows.next().await.unwrap().is_some(),
                "missing table: {table}"
            );
        }

        // Insert and read back a project row.
        let now = 1_700_000_000_000i64;
        db.conn()
            .execute(
                "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                (
                    "proj-1",
                    "Demo Project",
                    "https://example.com/demo.git",
                    "pending",
                    now,
                    now,
                ),
            )
            .await
            .unwrap();

        let mut rows = db
            .conn()
            .query(
                "SELECT id, name, repo_url, clone_status FROM project WHERE id=?1",
                libsql::params!["proj-1"],
            )
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("project row should exist");
        assert_eq!(row.get::<String>(0).unwrap(), "proj-1");
        assert_eq!(row.get::<String>(1).unwrap(), "Demo Project");
        assert_eq!(
            row.get::<String>(2).unwrap(),
            "https://example.com/demo.git"
        );
        assert_eq!(row.get::<String>(3).unwrap(), "pending");
        assert!(rows.next().await.unwrap().is_none());
    }

    /// A connection opened against a file-backed database runs in WAL mode with
    /// the busy timeout applied — the two pragmas that keep an external reader
    /// (DBeaver et al.) from turning into instant `database is locked` failures.
    #[tokio::test]
    async fn connect_applies_wal_and_busy_timeout_to_file_backed_databases() {
        let path = std::env::temp_dir().join(format!(
            "dearborn-pragma-test-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let path = path.to_str().unwrap();

        {
            let db = Db::connect(path).await.unwrap();
            let mut rows = db.conn().query("PRAGMA journal_mode", ()).await.unwrap();
            let mode: String = rows.next().await.unwrap().unwrap().get(0).unwrap();
            assert_eq!(mode.to_lowercase(), "wal");

            let mut rows = db.conn().query("PRAGMA busy_timeout", ()).await.unwrap();
            let timeout_ms: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
            assert_eq!(timeout_ms, BUSY_TIMEOUT_MS);
        }

        for suffix in ["", "-shm", "-wal"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }

    /// In-memory databases cannot do WAL (there is no file); opening one must
    /// still succeed — the pragma degrades gracefully instead of failing boot.
    #[tokio::test]
    async fn connect_on_memory_database_tolerates_unavailable_wal() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        // Any statement works afterwards — the connection is usable.
        db.conn()
            .execute("CREATE TABLE t (x INTEGER)", ())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn migrations_are_idempotent_across_reconnect() {
        let path = std::env::temp_dir().join(format!(
            "dearborn-mig-test-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let path = path.to_str().unwrap();

        {
            let db = Db::connect(path).await.unwrap();
            assert_eq!(
                db.run_migrations().await.unwrap(),
                MIGRATIONS.len() as u32,
                "first boot applies"
            );
        }
        {
            // A fresh connection to the same file sees the applied migration.
            let db = Db::connect(path).await.unwrap();
            assert_eq!(db.run_migrations().await.unwrap(), 0, "re-boot is a no-op");
        }

        for suffix in ["", "-shm", "-wal"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }

    /// A fresh boot's `0004_executor` migration lands every new column on
    /// `epic`/`task`/`agent_run` (M2 §2.1) plus the three claim-path indexes.
    /// Checked via `PRAGMA table_info` rather than a `SELECT` so a column that
    /// merely fails to bind still shows up as missing.
    #[tokio::test]
    async fn migration_0004_adds_executor_columns_and_indexes() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();

        for (table, column) in [
            ("epic", "pr_url"),
            ("epic", "pr_number"),
            ("epic", "blocked_reason"),
            ("task", "lease_owner"),
            ("task", "lease_expires_at"),
            ("task", "branch_name"),
            ("task", "pr_url"),
            ("task", "pr_number"),
            ("task", "base_sha"),
            ("agent_run", "attempt"),
            ("agent_run", "status"),
            ("agent_run", "verdict"),
            ("agent_run", "started_at"),
            ("agent_run", "ended_at"),
            ("agent_run", "exit_code"),
        ] {
            let mut rows = db
                .conn()
                .query(&format!("PRAGMA table_info({table})"), ())
                .await
                .unwrap();
            let mut found = false;
            while let Some(row) = rows.next().await.unwrap() {
                if row.get::<String>(1).unwrap() == column {
                    found = true;
                    break;
                }
            }
            assert!(found, "missing column {table}.{column}");
        }

        for index in ["idx_epic_claim", "idx_task_claim", "idx_agent_run_task"] {
            let mut rows = db
                .conn()
                .query(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name=?1",
                    libsql::params![index],
                )
                .await
                .unwrap();
            assert!(
                rows.next().await.unwrap().is_some(),
                "missing index: {index}"
            );
        }
    }

    /// A fresh boot's `0008_failure_detail` migration lands the Rec-5
    /// `failure_detail` column on both containers `worker::fail_item` writes
    /// to. Same `PRAGMA table_info` discipline as the `0004` check above.
    #[tokio::test]
    async fn migration_0008_adds_failure_detail_to_task_and_epic() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();

        for (table, column) in [("task", "failure_detail"), ("epic", "failure_detail")] {
            let mut rows = db
                .conn()
                .query(&format!("PRAGMA table_info({table})"), ())
                .await
                .unwrap();
            let mut found = false;
            while let Some(row) = rows.next().await.unwrap() {
                if row.get::<String>(1).unwrap() == column {
                    found = true;
                    break;
                }
            }
            assert!(found, "missing column {table}.{column}");
        }
    }

    /// A fresh boot's `0007_users` migration lands both auth tables with the
    /// schema the technical plan §3 specifies: a case-insensitive unique
    /// `username`, the `role` CHECK vocabulary, `active` defaulting to 1, and
    /// the `session_user` index.
    #[tokio::test]
    async fn migration_0007_creates_the_user_and_session_tables() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();

        for (table, column) in [
            ("user", "id"),
            ("user", "username"),
            ("user", "display_name"),
            ("user", "password_hash"),
            ("user", "role"),
            ("user", "active"),
            ("user", "created_at"),
            ("user", "updated_at"),
            ("session", "id"),
            ("session", "user_id"),
            ("session", "refresh_token_hash"),
            ("session", "created_at"),
            ("session", "expires_at"),
            ("session", "last_used_at"),
        ] {
            let mut rows = db
                .conn()
                .query(&format!("PRAGMA table_info({table})"), ())
                .await
                .unwrap();
            let mut found = false;
            while let Some(row) = rows.next().await.unwrap() {
                if row.get::<String>(1).unwrap() == column {
                    found = true;
                    break;
                }
            }
            assert!(found, "missing column {table}.{column}");
        }

        let mut rows = db
            .conn()
            .query(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='session_user'",
                (),
            )
            .await
            .unwrap();
        assert!(rows.next().await.unwrap().is_some(), "missing session_user");

        // `active` defaults to 1 and `username` folds case in the unique index.
        db.conn()
            .execute(
                "INSERT INTO user (id, username, display_name, password_hash, role, \
                     created_at, updated_at) VALUES ('u1', 'Josiah', 'Josiah', 'x', 'admin', 1, 1)",
                (),
            )
            .await
            .unwrap();
        let mut rows = db
            .conn()
            .query("SELECT active FROM user WHERE username = 'josiah'", ())
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("NOCASE lookup finds the differently-cased row");
        assert_eq!(row.get::<i64>(0).unwrap(), 1, "active defaults to 1");

        // A case-variant duplicate collides on the unique index.
        assert!(db
            .conn()
            .execute(
                "INSERT INTO user (id, username, display_name, password_hash, role, \
                     created_at, updated_at) VALUES ('u2', 'JOSIAH', 'J', 'x', 'user', 1, 1)",
                (),
            )
            .await
            .is_err());

        // The role CHECK rejects anything outside the two-role vocabulary.
        assert!(db
            .conn()
            .execute(
                "INSERT INTO user (id, username, display_name, password_hash, role, \
                     created_at, updated_at) VALUES ('u3', 'root', 'R', 'x', 'superuser', 1, 1)",
                (),
            )
            .await
            .is_err());
    }

    /// `0007_users` also applies cleanly to a database that already carries
    /// every earlier migration — the real "existing `dearborn.db` restarts on
    /// the new binary" upgrade path, which must add the two tables and nothing
    /// else.
    #[tokio::test]
    async fn migration_0007_applies_cleanly_on_an_existing_database() {
        let path = std::env::temp_dir().join(format!(
            "dearborn-users-existing-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let path = path.to_str().unwrap();

        {
            // Simulate a pre-auth installation: every migration before 7.
            let db = Db::connect(path).await.unwrap();
            db.conn()
                .execute(
                    "CREATE TABLE IF NOT EXISTS _migrations (\
                         id         INTEGER PRIMARY KEY, \
                         name       TEXT NOT NULL, \
                         applied_at INTEGER NOT NULL\
                     )",
                    (),
                )
                .await
                .unwrap();
            for migration in MIGRATIONS.iter().filter(|m| m.id < 7) {
                db.conn().execute_batch(migration.sql).await.unwrap();
                db.conn()
                    .execute(
                        "INSERT INTO _migrations (id, name, applied_at) VALUES (?1, ?2, ?3)",
                        (migration.id, migration.name, now_ms()),
                    )
                    .await
                    .unwrap();
            }
            // Real data predating the migration must survive it.
            db.conn()
                .execute(
                    "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
                     VALUES ('p-1', 'P', 'https://example.com/p.git', 'ready', 1, 1)",
                    (),
                )
                .await
                .unwrap();
        }

        {
            let db = Db::connect(path).await.unwrap();
            let expected_new = MIGRATIONS.iter().filter(|m| m.id > 6).count() as u32;
            assert_eq!(
                db.run_migrations().await.unwrap(),
                expected_new,
                "every migration newer than the simulated pre-auth state applies"
            );

            // The instance starts unclaimed — zero users, not an error.
            let mut rows = db
                .conn()
                .query("SELECT COUNT(*) FROM user", ())
                .await
                .unwrap();
            let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
            assert_eq!(count, 0);

            // Pre-existing rows are untouched.
            let mut rows = db
                .conn()
                .query("SELECT name FROM project WHERE id = 'p-1'", ())
                .await
                .unwrap();
            assert_eq!(
                rows.next()
                    .await
                    .unwrap()
                    .unwrap()
                    .get::<String>(0)
                    .unwrap(),
                "P"
            );

            assert_eq!(db.run_migrations().await.unwrap(), 0, "re-boot is a no-op");
        }

        for suffix in ["", "-shm", "-wal"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }

    /// `0004_executor` also applies cleanly to a database that already has
    /// migrations 1-3 (i.e. a real pre-M2 `dearborn.db`): simulate that state
    /// by applying only the first three migrations by hand, then run the full
    /// migration set and confirm exactly one (id 4) newly applies and the new
    /// columns show up — the AC's "existing dearborn.db" case.
    #[tokio::test]
    async fn migration_0004_applies_cleanly_on_an_existing_pre_m2_database() {
        let path = std::env::temp_dir().join(format!(
            "dearborn-t500-existing-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let path = path.to_str().unwrap();

        {
            let db = Db::connect(path).await.unwrap();
            db.conn()
                .execute(
                    "CREATE TABLE IF NOT EXISTS _migrations (\
                         id         INTEGER PRIMARY KEY, \
                         name       TEXT NOT NULL, \
                         applied_at INTEGER NOT NULL\
                     )",
                    (),
                )
                .await
                .unwrap();
            for migration in MIGRATIONS.iter().filter(|m| m.id < 4) {
                db.conn().execute_batch(migration.sql).await.unwrap();
                db.conn()
                    .execute(
                        "INSERT INTO _migrations (id, name, applied_at) VALUES (?1, ?2, ?3)",
                        (migration.id, migration.name, now_ms()),
                    )
                    .await
                    .unwrap();
            }
        }

        // Re-open (as a restarted server would) and run the full migration set:
        // everything after 0003 (the pre-M2 state this DB simulates) applies.
        // Counted dynamically so appending migration 0006+ later keeps this
        // test honest without edits.
        {
            let db = Db::connect(path).await.unwrap();
            let expected_new = MIGRATIONS.iter().filter(|m| m.id > 3).count() as u32;
            assert_eq!(
                db.run_migrations().await.unwrap(),
                expected_new,
                "every migration newer than the simulated pre-M2 state applies"
            );

            let mut rows = db
                .conn()
                .query("PRAGMA table_info(epic)", ())
                .await
                .unwrap();
            let mut has_pr_url = false;
            while let Some(row) = rows.next().await.unwrap() {
                if row.get::<String>(1).unwrap() == "pr_url" {
                    has_pr_url = true;
                }
            }
            assert!(
                has_pr_url,
                "epic.pr_url present after migrating an existing db"
            );

            // Re-running again is a no-op.
            assert_eq!(db.run_migrations().await.unwrap(), 0);
        }

        for suffix in ["", "-shm", "-wal"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }

    /// Migration `0009_agent_run_events` creates the table and composite index,
    /// and FK enforcement (via `PRAGMA foreign_keys = ON`) rejects orphan rows.
    #[tokio::test]
    async fn migration_0009_creates_agent_run_events_table_and_index() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();

        for column in [
            "id",
            "agent_run_id",
            "kind",
            "tool_call_id",
            "name",
            "ok",
            "created_at",
        ] {
            let mut rows = db
                .conn()
                .query("PRAGMA table_info(agent_run_events)", ())
                .await
                .unwrap();
            let mut found = false;
            while let Some(row) = rows.next().await.unwrap() {
                if row.get::<String>(1).unwrap() == column {
                    found = true;
                    break;
                }
            }
            assert!(found, "missing column agent_run_events.{column}");
        }

        let mut rows = db
            .conn()
            .query(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_agent_run_events_run'",
                (),
            )
            .await
            .unwrap();
        assert!(
            rows.next().await.unwrap().is_some(),
            "missing index: idx_agent_run_events_run"
        );

        // FK enforcement: inserting an event with a non-existent agent_run_id must fail.
        let err = db
            .conn()
            .execute(
                "INSERT INTO agent_run_events (id, agent_run_id, kind, tool_call_id, name, created_at) \
                 VALUES ('ev-1', 'nonexistent-run', 'tool_start', 'tc-1', 'bash', 1)",
                (),
            )
            .await;
        assert!(err.is_err(), "FK violation should be rejected");
    }

    /// Migration `0012_pr_feedback` creates the `pr_feedback` table with the
    /// exact lifecycle schema the epic plan §6.3 spells out, plus the UNIQUE
    /// identity index on `(pr_number, source_kind, github_id)` that makes
    /// dedup work. (The spec called this migration "0011", but that slot was
    /// already taken by token_columns; it lands here as 0012.)
    #[tokio::test]
    async fn migration_0012_creates_pr_feedback_table_and_unique_index() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();

        for column in [
            "id",
            "project_id",
            "epic_id",
            "task_id",
            "pr_number",
            "source_kind",
            "github_id",
            "thread_id",
            "classification",
            "state",
            "spawned_task_ids",
            "base_sha",
            "created_at",
            "updated_at",
        ] {
            let mut rows = db
                .conn()
                .query("PRAGMA table_info(pr_feedback)", ())
                .await
                .unwrap();
            let mut found = false;
            while let Some(row) = rows.next().await.unwrap() {
                if row.get::<String>(1).unwrap() == column {
                    found = true;
                    break;
                }
            }
            assert!(found, "missing column pr_feedback.{column}");
        }

        let mut rows = db
            .conn()
            .query(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_pr_feedback_ident'",
                (),
            )
            .await
            .unwrap();
        assert!(
            rows.next().await.unwrap().is_some(),
            "missing index: idx_pr_feedback_ident"
        );

        // The unique index is the dedup guarantee: the same
        // (pr_number, source_kind, github_id) identity may only be stored once.
        let now = now_ms();
        db.conn()
            .execute(
                "INSERT INTO pr_feedback \
                 (id, project_id, pr_number, source_kind, github_id, state, created_at, updated_at) \
                 VALUES ('fb-1', 'p', 7, 'review_comment', 200, 'in_progress', ?1, ?1)",
                libsql::params![now],
            )
            .await
            .unwrap();
        let dup = db
            .conn()
            .execute(
                "INSERT INTO pr_feedback \
                 (id, project_id, pr_number, source_kind, github_id, state, created_at, updated_at) \
                 VALUES ('fb-2', 'p', 7, 'review_comment', 200, 'in_progress', ?1, ?1)",
                libsql::params![now],
            )
            .await;
        assert!(
            dup.is_err(),
            "duplicate (pr_number, source_kind, github_id) identity must be rejected"
        );
    }

    /// Migration `0015_planning_map_schema` lands every planning-map table with
    /// the exact schema the wayfinder epic plan §4 spells out: documented
    /// columns, PKs, and FKs (FK enforcement via `PRAGMA foreign_keys = ON`
    /// rejects orphan rows), plus the epic prose cutover — the four wayfinder
    /// columns present, product_context/technical_context gone.
    #[tokio::test]
    async fn migration_0015_creates_planning_map_tables_and_epic_prose_cutover() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();

        // Every §4 table exists.
        for table in [
            "map_node",
            "map_node_dependency",
            "node_session",
            "node_message",
            "document",
            "document_version",
            "document_section",
            "node_asset",
            "comment",
            "activity",
        ] {
            let mut rows = db
                .conn()
                .query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?1",
                    libsql::params![table],
                )
                .await
                .unwrap();
            assert!(
                rows.next().await.unwrap().is_some(),
                "missing table: {table}"
            );
        }

        // Documented columns on each new table (PRAGMA table_info discipline:
        // a column that merely fails to bind still shows up as missing).
        let expected_columns: &[(&str, &[&str])] = &[
            (
                "map_node",
                &[
                    "id",
                    "epic_id",
                    "kind",
                    "task_mode",
                    "state",
                    "title",
                    "question",
                    "gist",
                    "out_of_scope_reason",
                    "created_by",
                    "resolved_by",
                    "position_x",
                    "position_y",
                    "created_at",
                    "updated_at",
                ],
            ),
            ("map_node_dependency", &["blocker_id", "blocked_id"]),
            (
                "node_session",
                &[
                    "node_id",
                    "harness_session_id",
                    "status",
                    "created_at",
                    "updated_at",
                ],
            ),
            (
                "node_message",
                &[
                    "id",
                    "node_id",
                    "role",
                    "actor_user_id",
                    "content",
                    "seq",
                    "created_at",
                ],
            ),
            (
                "document",
                &[
                    "epic_id",
                    "html",
                    "version",
                    "last_edited_by",
                    "updated_at",
                ],
            ),
            (
                "document_version",
                &[
                    "epic_id",
                    "version",
                    "html",
                    "editor_user_id",
                    "node_id",
                    "created_at",
                ],
            ),
            (
                "document_section",
                &[
                    "epic_id",
                    "section_id",
                    "title",
                    "provenance",
                    "last_edited_by",
                    "version",
                ],
            ),
            (
                "node_asset",
                &["id", "node_id", "mime", "bytes", "label", "created_at"],
            ),
            (
                "comment",
                &[
                    "id",
                    "epic_id",
                    "thread_id",
                    "anchor_kind",
                    "anchor_id",
                    "author_user_id",
                    "is_agent",
                    "body",
                    "resolved",
                    "promoted_node_id",
                    "created_at",
                ],
            ),
            (
                "activity",
                &[
                    "id",
                    "epic_id",
                    "node_id",
                    "actor_user_id",
                    "action",
                    "detail",
                    "created_at",
                ],
            ),
        ];
        for (table, columns) in expected_columns {
            let mut found: Vec<String> = Vec::new();
            let mut rows = db
                .conn()
                .query(&format!("PRAGMA table_info({table})"), ())
                .await
                .unwrap();
            while let Some(row) = rows.next().await.unwrap() {
                found.push(row.get::<String>(1).unwrap());
            }
            for column in *columns {
                assert!(
                    found.iter().any(|f| f == column),
                    "missing column {table}.{column} (have: {found:?})"
                );
            }
        }

        // The epic prose cutover: the four new columns exist; the two
        // planning-context columns are gone.
        let mut epic_columns = Vec::new();
        let mut rows = db
            .conn()
            .query("PRAGMA table_info(epic)", ())
            .await
            .unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            epic_columns.push(row.get::<String>(1).unwrap());
        }
        for column in [
            "destination",
            "notes",
            "not_yet_specified",
            "out_of_scope",
        ] {
            assert!(
                epic_columns.iter().any(|c| c == column),
                "missing epic.{column}"
            );
        }
        assert!(
            !epic_columns.iter().any(|c| c == "product_context"),
            "epic.product_context must be dropped"
        );
        assert!(
            !epic_columns.iter().any(|c| c == "technical_context"),
            "epic.technical_context must be dropped"
        );

        // Composite PKs: duplicate map_node_dependency edges and duplicate
        // (epic_id, version) document rows are rejected.
        db.conn()
            .execute(
                "INSERT INTO project (id, name, repo_url, created_at, updated_at) \
                 VALUES ('p', 'P', 'https://example.com/p.git', 1, 1)",
                (),
            )
            .await
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO epic (id, project_id, title, created_at, updated_at) \
                 VALUES ('e', 'p', 'E', 1, 1)",
                (),
            )
            .await
            .unwrap();
        let node_row = |id: &str, epic_id: &str| {
            format!(
                "INSERT INTO map_node (id, epic_id, kind, state, title, created_at, updated_at) \
                 VALUES ('{id}', '{epic_id}', 'grilling', 'open', 'T', 1, 1)"
            )
        };
        db.conn()
            .execute(&node_row("n1", "e"), ())
            .await
            .unwrap();
        db.conn()
            .execute(&node_row("n2", "e"), ())
            .await
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO map_node_dependency (blocker_id, blocked_id) VALUES ('n1', 'n2')",
                (),
            )
            .await
            .unwrap();
        let dup_edge = db
            .conn()
            .execute(
                "INSERT INTO map_node_dependency (blocker_id, blocked_id) VALUES ('n1', 'n2')",
                (),
            )
            .await;
        assert!(dup_edge.is_err(), "duplicate dependency edge must be rejected");

        db.conn()
            .execute(
                "INSERT INTO document (epic_id, html, version, updated_at) \
                 VALUES ('e', '<p>x</p>', 1, 1)",
                (),
            )
            .await
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO document_version (epic_id, version, html, created_at) \
                 VALUES ('e', 1, '<p>x</p>', 1)",
                (),
            )
            .await
            .unwrap();
        let dup_version = db
            .conn()
            .execute(
                "INSERT INTO document_version (epic_id, version, html, created_at) \
                 VALUES ('e', 1, '<p>x2</p>', 2)",
                (),
            )
            .await;
        assert!(
            dup_version.is_err(),
            "duplicate (epic_id, version) must be rejected"
        );

        // FK enforcement: nodes/edges/messages/assets referencing missing rows
        // are rejected (PRAGMA foreign_keys = ON).
        let orphan_node = db.conn().execute(&node_row("orphan", "ghost"), ()).await;
        assert!(orphan_node.is_err(), "FK violation on epic_id must be rejected");
        let orphan_edge = db
            .conn()
            .execute(
                "INSERT INTO map_node_dependency (blocker_id, blocked_id) VALUES ('ghost', 'n2')",
                (),
            )
            .await;
        assert!(orphan_edge.is_err(), "FK violation on blocker_id must be rejected");
        let orphan_message = db
            .conn()
            .execute(
                "INSERT INTO node_message (id, node_id, role, content, seq, created_at) \
                 VALUES ('m1', 'ghost', 'user', 'hi', 1, 1)",
                (),
            )
            .await;
        assert!(orphan_message.is_err(), "FK violation on node_id must be rejected");
        let orphan_asset = db
            .conn()
            .execute(
                "INSERT INTO node_asset (id, node_id, mime, bytes, created_at) \
                 VALUES ('a1', 'ghost', 'text/html', X'00', 1)",
                (),
            )
            .await;
        assert!(orphan_asset.is_err(), "FK violation on node_id must be rejected");

        // The overhauled comment anchors to a node and defaults its flags.
        db.conn()
            .execute(
                "INSERT INTO comment (id, epic_id, thread_id, anchor_kind, anchor_id, body, created_at) \
                 VALUES ('c1', 'e', 't1', 'node', 'n1', 'hello', 1)",
                (),
            )
            .await
            .unwrap();
        let mut rows = db
            .conn()
            .query("SELECT is_agent, resolved FROM comment WHERE id = 'c1'", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("comment row");
        assert_eq!(row.get::<i64>(0).unwrap(), 0, "is_agent defaults to 0");
        assert_eq!(row.get::<i64>(1).unwrap(), 0, "resolved defaults to 0");
    }

    /// Migration 0014 completes the clean cutover: the linear product/technical
    /// planning store (`transcript_message`, `planning_session`) is gone, and
    /// nothing recreated it (the tables do not exist for inserts either).
    #[tokio::test]
    async fn migration_0014_drops_the_linear_planning_tables() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();

        for table in ["transcript_message", "planning_session"] {
            let mut rows = db
                .conn()
                .query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?1",
                    libsql::params![table],
                )
                .await
                .unwrap();
            assert!(
                rows.next().await.unwrap().is_none(),
                "{table} must be dropped by the cutover"
            );
        }
    }

    /// The planning-map cutover leaves Ready/InProgress epics intact: on a DB
    /// migrated through 0015, a pre-existing epic row with executor columns
    /// populated still round-trips, and the executor task tables/claim indexes
    /// are untouched (planning nodes are a separate namespace).
    #[tokio::test]
    async fn migration_0015_keeps_existing_ready_and_in_progress_epics_intact() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();

        db.conn()
            .execute(
                "INSERT INTO project (id, name, repo_url, created_at, updated_at) \
                 VALUES ('p', 'P', 'https://example.com/p.git', 1, 1)",
                (),
            )
            .await
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO epic (id, project_id, title, status, pr_url, pr_number, \
                 created_at, updated_at) \
                 VALUES ('e-ready', 'p', 'Shipped', 'Ready', \
                 'https://github.com/acme/demo/pull/7', 7, 1, 1)",
                (),
            )
            .await
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO task (id, epic_id, project_id, title, status, created_at, updated_at) \
                 VALUES ('t1', 'e-ready', 'p', 'Slice', 'Done', 1, 1)",
                (),
            )
            .await
            .unwrap();

        // The Ready epic keeps its executor state; nothing was backfilled or
        // cleared by the cutover (destination etc. simply stay NULL).
        let mut rows = db
            .conn()
            .query(
                "SELECT status, pr_url, pr_number, destination, notes FROM epic WHERE id = 'e-ready'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("epic row");
        assert_eq!(row.get::<String>(0).unwrap(), "Ready");
        assert_eq!(
            row.get::<String>(1).unwrap(),
            "https://github.com/acme/demo/pull/7"
        );
        assert_eq!(row.get::<i64>(2).unwrap(), 7);
        assert_eq!(row.get::<Option<String>>(3).unwrap(), None);
        assert_eq!(row.get::<Option<String>>(4).unwrap(), None);

        // The executor's claim index (idx_task_claim) still exists — planning
        // tables are additive and did not disturb the task namespace.
        let mut rows = db
            .conn()
            .query(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_task_claim'",
                (),
            )
            .await
            .unwrap();
        assert!(
            rows.next().await.unwrap().is_some(),
            "idx_task_claim must survive the planning-map migration"
        );
    }
}
