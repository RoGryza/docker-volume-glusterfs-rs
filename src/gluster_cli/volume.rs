use std::io::Cursor;
use std::process::Command;

use log::warn;
use serde::Deserialize;
use serde_repr::Deserialize_repr;

use crate::util::Utf8Lossy;

#[derive(Debug, Deserialize)]
#[serde(try_from = "de::CliOutput")]
pub struct Info {
    pub volumes: Vec<Volume>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    pub name: String,
    pub id: String,
    #[serde(rename = "status")]
    pub status: VolumeStatus,
    #[serde(rename = "type")]
    pub volume_type: VolumeType,
}

#[derive(Debug, Clone, Copy, Deserialize_repr)]
#[repr(u8)]
pub enum VolumeType {
    Distribute = 0,
    Replicate = 2,
}

#[derive(Debug, Clone, Copy, Deserialize_repr)]
#[repr(u8)]
pub enum VolumeStatus {
    Created = 0,
    Started = 1,
}

mod de {
    #![allow(non_snake_case)]
    use super::*;
    use std::convert::TryFrom;

    #[derive(Deserialize)]
    pub struct CliOutput {
        opRet: i32,
        opErrno: i32,
        #[serde(default)]
        opErrstr: String,
        volInfo: VolInfo,
    }
    #[derive(Deserialize)]
    pub struct VolInfo {
        volumes: Volumes,
    }
    #[derive(Deserialize)]
    pub struct Volumes {
        #[serde(default)]
        volume: Vec<Volume>,
    }

    impl TryFrom<CliOutput> for Info {
        type Error = String;

        fn try_from(o: CliOutput) -> Result<Self, String> {
            if o.opRet != 0 || o.opErrno != 0 {
                Err(o.opErrstr)
            } else {
                Ok(Info {
                    volumes: o.volInfo.volumes.volume,
                })
            }
        }
    }
}

pub async fn info() -> Result<Info, String> {
    let mut cmd = Command::new("gluster");
    cmd.arg("--mode=script")
        .arg("--xml")
        .arg("volume")
        .arg("info");

    let output = cmd.output().map_err(|e| e.to_string())?;
    if output.status.success() {
        if !output.stderr.is_empty() {
            warn!("gluster stderr: {}", Utf8Lossy(&output.stderr));
        }
        Ok(serde_xml_rs::from_reader(Cursor::new(output.stdout)).map_err(|e| e.to_string())?)
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}
