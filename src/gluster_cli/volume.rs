use std::convert::TryFrom;
use std::io::Cursor;
use std::process::Command;

use log::warn;
use serde::Deserialize;

use crate::util::Utf8Lossy;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash)]
pub struct VolumeId(String);

#[derive(Debug, Deserialize)]
#[serde(try_from = "de::InfoCliOutput")]
pub struct Info {
    pub volumes: Vec<Volume>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    pub name: String,
    pub id: VolumeId,
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

pub async fn create(
    name: &str,
    replica: u32,
    bricks: &[(&str, &str)],
    force: bool,
) -> Result<VolumeId, String> {
    let mut cmd = Command::new("gluster");
    cmd.arg("--mode=script")
        .arg("--xml")
        .arg("volume")
        .arg("create")
        .arg(name)
        .arg("replica")
        .arg(replica.to_string());
    for (host, path) in bricks {
        cmd.arg(format!("{}:{}", host, path));
    }
    if force {
        cmd.arg("force");
    }

    let output = cmd.output().map_err(|e| e.to_string())?;
    if output.status.success() {
        if !output.stderr.is_empty() {
            warn!("gluster stderr: {}", Utf8Lossy(&output.stderr));
        }
        let resp: de::CreateCliOutput =
            serde_xml_rs::from_reader(Cursor::new(output.stdout)).map_err(|e| e.to_string())?;
        Ok(Volume::try_from(resp)?.id)
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}

mod de {
    #![allow(non_snake_case)]
    use super::*;
    use std::convert::TryFrom;

    #[derive(Deserialize)]
    pub struct InfoCliOutput {
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

    #[derive(Deserialize)]
    pub struct CreateCliOutput {
        opRet: i32,
        opErrno: i32,
        #[serde(default)]
        opErrstr: String,
        volume: Volume,
    }

    impl TryFrom<InfoCliOutput> for Info {
        type Error = String;

        fn try_from(o: InfoCliOutput) -> Result<Self, String> {
            if o.opRet != 0 || o.opErrno != 0 {
                Err(o.opErrstr)
            } else {
                Ok(Info {
                    volumes: o.volInfo.volumes.volume,
                })
            }
        }
    }

    impl TryFrom<CreateCliOutput> for Volume {
        type Error = String;

        fn try_from(o: CreateCliOutput) -> Result<Self, String> {
            if o.opRet != 0 || o.opErrno != 0 {
                Err(o.opErrstr)
            } else {
                Ok(o.volume)
            }
        }
    }
}
