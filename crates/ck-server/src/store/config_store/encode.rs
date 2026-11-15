//! Complete-state encoding into a fresh, private SQLite file. The destination is
//! an empty output supplied by the storage adapter, never a previous database.
use super::*;

impl State {
    pub(super) fn encode(&self, destination: &Path) -> anyhow::Result<()> {
        anyhow::ensure!(
            std::fs::metadata(destination)?.len() == 0,
            "encoder output must be a new empty file"
        );
        let mut conn = Connection::open_with_flags(destination, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        db::initialize(&mut conn, true, true)?;
        self.write_rows(&mut conn)?;
        conn.close().map_err(|(_, error)| error)?;
        Ok(())
    }

    pub(super) fn write_rows(&self, conn: &mut Connection) -> anyhow::Result<()> {
        let tx = conn.transaction()?;
        {
            let mut insert = tx.prepare(
                "INSERT INTO resource(kind,id,ord,spec,description,rev,created_rev) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            )?;
            for resource in self.resources.iter() {
                insert.execute(params![
                    resource.kind.name(),
                    resource.id,
                    resource.ord,
                    resource.spec.to_string(),
                    resource.description,
                    i64::try_from(resource.rev)?,
                    i64::try_from(resource.created_rev)?
                ])?;
            }
        }
        {
            let mut insert = tx.prepare("INSERT INTO revision(rev,at,actor,message,digest) VALUES (?1,?2,?3,?4,?5)")?;
            for revision in self.revisions.values() {
                insert.execute(params![
                    i64::try_from(revision.rev)?,
                    revision.at,
                    revision.actor,
                    revision.message,
                    revision.digest
                ])?;
            }
        }
        {
            let mut insert = tx.prepare("INSERT INTO change(rev,kind,id,before,after) VALUES (?1,?2,?3,?4,?5)")?;
            for (rev, changes) in &self.changes {
                for change in changes.iter() {
                    insert.execute(params![
                        i64::try_from(*rev)?,
                        change.kind.name(),
                        change.id,
                        change.before,
                        change.after
                    ])?;
                }
            }
        }
        {
            let mut insert = tx.prepare("INSERT INTO credential_document(name,bytes,rev) VALUES (?1,?2,?3)")?;
            for (name, document) in &self.documents {
                insert.execute(params![name, document.bytes.as_slice(), document.rev])?;
            }
        }
        {
            let mut insert = tx.prepare("INSERT INTO credential_meta(name,value) VALUES (?1,?2)")?;
            for (name, value) in &self.metadata {
                insert.execute(params![name, value])?;
            }
            for (name, value) in self.envelope_keys.rows() {
                insert.execute(params![name, value])?;
            }
        }
        {
            let mut insert = tx.prepare("INSERT INTO credential_cooldown(id,record) VALUES (?1,?2)")?;
            for (id, record) in &self.cooldowns {
                insert.execute(params![id, record])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}
