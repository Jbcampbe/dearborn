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
const MIGRATIONS: &[Migration] = &[Migration {
    id: 1,
    name: "0001_baseline",
    sql: include_str!("../migrations/0001_baseline.sql"),
}];

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

        // Fresh boot applies the single baseline migration.
        assert_eq!(db.run_migrations().await.unwrap(), 1);
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
        let row = rows.next().await.unwrap().expect("project row should exist");
        assert_eq!(row.get::<String>(0).unwrap(), "proj-1");
        assert_eq!(row.get::<String>(1).unwrap(), "Demo Project");
        assert_eq!(row.get::<String>(2).unwrap(), "https://example.com/demo.git");
        assert_eq!(row.get::<String>(3).unwrap(), "pending");
        assert!(rows.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn migrations_are_idempotent_across_reconnect() {
        let path = std::env::temp_dir().join(format!(
            "deerborn-mig-test-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let path = path.to_str().unwrap();

        {
            let db = Db::connect(path).await.unwrap();
            assert_eq!(db.run_migrations().await.unwrap(), 1, "first boot applies");
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
}
