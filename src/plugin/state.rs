// TODO maybe use SQLite asynchronously
// TODO maybe throttle refreshes
use std::collections::HashMap;
use std::mem;
use std::num::NonZeroU64;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::Arc;

use futures::future::try_join_all;
use futures::lock::Mutex;
use rusqlite::NO_PARAMS;

use crate::heketi::{self, VolumeId};
use crate::plugin::Result;

pub struct State {
    db_path: PathBuf,
    heketi: Arc<heketi::Client>,
    volume_ids: Mutex<HashMap<String, VolumeId>>,
}

#[derive(Debug, Clone)]
pub struct VolumeState {
    pub id: VolumeId,
    pub mount: Option<VolumeMount>,
}

#[derive(Debug, Clone)]
pub struct VolumeMount {
    pub path: PathBuf,
    count: NonZeroU64,
}

const MIGRATE: &'static str = r#"
CREATE TABLE IF NOT EXISTS volume_mounts (
    volume_id TEXT PRIMARY KEY,
    mountpoint TEXT NOT NULL,
    n INT NOT NULL CHECK (n > 0)
)"#;

impl State {
    pub async fn read<P>(db_path: P, heketi: Arc<heketi::Client>) -> Result<Self>
    where
        P: Into<PathBuf>,
    {
        let db_path = db_path.into();
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute(MIGRATE, NO_PARAMS)?;

        let st = State {
            db_path,
            heketi,
            volume_ids: Mutex::new(HashMap::new()),
        };
        st.refresh().await?;
        Ok(st)
    }

    pub async fn get(&self, name: &str) -> Result<Option<VolumeState>> {
        let id = self.get_id_refresh(name).await?;
        Ok(id.map(|id| VolumeState { id, mount: None }))
    }

    pub async fn set_id(&self, name: String, id: VolumeId) -> Result<()> {
        let mut volume_ids = self.volume_ids.lock().await;
        volume_ids.insert(name, id);
        Ok(())
    }

    pub async fn pop_id(&self, name: &str) -> Result<Option<VolumeId>> {
        {
            let volume_ids = self.volume_ids.lock().await;
            if let Some(id) = volume_ids.get(name) {
                return Ok(Some(id.clone()));
            }
        }
        // If the volume wasn't found we may be out of sync with heketi
        self.refresh().await?;
        let mut volume_ids = self.volume_ids.lock().await;
        Ok(volume_ids.remove(name))
    }

    pub async fn list(&self) -> Result<HashMap<String, VolumeState>> {
        // TODO stream results
        self.refresh().await?;
        let volume_ids = self.volume_ids.lock().await;
        let states = volume_ids
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    VolumeState {
                        id: v.clone(),
                        mount: None,
                    },
                )
            })
            .collect();
        Ok(states)
    }

    async fn get_id_refresh(&self, name: &str) -> Result<Option<VolumeId>> {
        {
            let volume_ids = self.volume_ids.lock().await;
            if let Some(id) = volume_ids.get(name) {
                return Ok(Some(id.clone()));
            }
        }
        // If the volume wasn't found we may be out of sync with heketi
        self.refresh().await?;
        let volume_ids = self.volume_ids.lock().await;
        Ok(volume_ids.get(name).cloned())
    }

    async fn refresh(&self) -> Result<()> {
        let mut volume_ids = HashMap::new();
        let volumes = try_join_all(
            self.heketi
                .list_volumes()
                .await?
                .iter()
                .map(|id| self.heketi.get_volume(id)),
        )
        .await?;
        for v in volumes {
            volume_ids.insert(v.name, v.id);
        }
        let _ = mem::replace(self.volume_ids.lock().await.deref_mut(), volume_ids);
        Ok(())
    }
}
