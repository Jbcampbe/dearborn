//! Minimal `agent_run` evidence writes (T-511's slice of a table T-512 owns).
//!
//! `agent_run` becomes the per-stage evidence table for the whole executor
//! (Milestone 2 §2.1/§2.2): T-512 will open a `running` row before a stage
//! starts, stream live output into it, and close it with a capped
//! (~256 KB head+tail) log, `session_id`, `verdict`, and `exit_code` — for
//! every stage, agent and non-agent alike.
//!
//! T-511 only needs one slice of that: a `setup_cmd` failure must land its
//! captured output somewhere a human can read after the epic goes
//! `Blocked(setup_failed)`. Rather than invent a separate ad-hoc log column,
//! this writes directly into `agent_run` — the table T-512 is about to build
//! the real machinery around — so there is one evidence table, not two. This
//! module is deliberately minimal: one write, after the fact (`setup_cmd` has
//! already finished by the time [`record_setup_run`] is called), with
//! `session_id` NULL (no agent involved), `attempt` fixed at `1` (`setup_cmd`
//! does not retry), and no capping (T-520's `DEARBORN_CMD_TIMEOUT_SECS` work
//! is where output capping for shell-command stages belongs). T-512 extends
//! this table's write path for every other stage; this function is not
//! expected to survive that refactor unchanged, but the table shape it writes
//! into is the real one.

use libsql::{params, Connection};

/// The `setup` stage's evidence row (see the module doc for scope).
pub struct SetupRunRecord<'a> {
    pub epic_id: &'a str,
    /// `"ok"` | `"error"` (§2.1's `agent_run.status` vocabulary).
    pub status: &'a str,
    pub exit_code: Option<i32>,
    /// Already redacted (see [`crate::git::redact`]) — never the raw command
    /// output if a PAT could have leaked into it.
    pub log: &'a str,
    pub started_at: i64,
    pub ended_at: i64,
}

/// Insert one `agent_run` row for the `setup` stage. `task_id` is `NULL`
/// (setup runs once per epic workspace, not per task) and `verdict` is `NULL`
/// (only the `review` stage ever sets one, T-530+).
pub async fn record_setup_run(
    conn: &Connection,
    rec: SetupRunRecord<'_>,
) -> Result<(), libsql::Error> {
    let id = ulid::Ulid::new().to_string();
    conn.execute(
        "INSERT INTO agent_run \
         (id, task_id, epic_id, stage, session_id, log, created_at, \
          attempt, status, verdict, started_at, ended_at, exit_code) \
         VALUES (?1, NULL, ?2, 'setup', NULL, ?3, ?4, 1, ?5, NULL, ?6, ?7, ?8)",
        params![
            id,
            rec.epic_id,
            rec.log,
            rec.started_at,
            rec.status,
            rec.started_at,
            rec.ended_at,
            rec.exit_code.map(|c| c as i64),
        ],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Db;

    #[tokio::test]
    async fn records_a_setup_stage_row_with_the_documented_shape() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let conn = db.conn();

        // agent_run.epic_id is a foreign key — seed a real project + epic row.
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
             VALUES ('proj-1', 'P', 'https://example.com/p.git', 'ready', 0, 0)",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES ('epic-1', 'proj-1', 'E', 'InProgress', 0, 0)",
            (),
        )
        .await
        .unwrap();

        record_setup_run(
            conn,
            SetupRunRecord {
                epic_id: "epic-1",
                status: "error",
                exit_code: Some(1),
                log: "boom",
                started_at: 1000,
                ended_at: 1500,
            },
        )
        .await
        .unwrap();

        let mut rows = conn
            .query(
                "SELECT task_id, epic_id, stage, session_id, log, attempt, status, \
                 verdict, started_at, ended_at, exit_code FROM agent_run WHERE epic_id = ?1",
                params!["epic-1"],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("row inserted");
        assert_eq!(row.get::<Option<String>>(0).unwrap(), None, "task_id NULL");
        assert_eq!(row.get::<String>(1).unwrap(), "epic-1");
        assert_eq!(row.get::<String>(2).unwrap(), "setup");
        assert_eq!(row.get::<Option<String>>(3).unwrap(), None, "session_id NULL");
        assert_eq!(row.get::<String>(4).unwrap(), "boom");
        assert_eq!(row.get::<i64>(5).unwrap(), 1);
        assert_eq!(row.get::<String>(6).unwrap(), "error");
        assert_eq!(row.get::<Option<String>>(7).unwrap(), None, "verdict NULL");
        assert_eq!(row.get::<i64>(8).unwrap(), 1000);
        assert_eq!(row.get::<i64>(9).unwrap(), 1500);
        assert_eq!(row.get::<Option<i64>>(10).unwrap(), Some(1));
    }
}
