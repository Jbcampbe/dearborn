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
            "transcript_message",
            "agent_run",
            "comment",
            "planning_session",
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
}
