/// Runtime capability toggles of the `[s3]` config section (FR-021).
///
/// # Examples
///
/// ```rust
/// use tinio_server::backend::Capabilities;
///
/// let caps = Capabilities::default();
/// assert!(caps.multipart && caps.copy_object);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// Multipart operations + UploadPartCopy.
    pub multipart: bool,
    /// CopyObject.
    pub copy_object: bool,
    /// ListObjects (V1).
    pub list_objects_v1: bool,
    /// ListObjectsV2.
    pub list_objects_v2: bool,
    /// DeleteObjects (batch).
    pub delete_objects: bool,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            multipart: true,
            copy_object: true,
            list_objects_v1: true,
            list_objects_v2: true,
            delete_objects: true,
        }
    }
}

/// The `[s3]` config section mapped onto the runtime switches (FR-021) —
/// the wiring point for the server startup orchestration: build the
/// backend with `Capabilities::from(config.s3)` so the configured toggles
/// take effect (a disabled group answers `NotImplemented`). `Default`
/// (everything on) stays the fallback for code paths without a config.
impl From<tinio_config::s3::Config> for Capabilities {
    fn from(config: tinio_config::s3::Config) -> Self {
        Self {
            multipart: config.multipart,
            copy_object: config.copy_object,
            list_objects_v1: config.list_objects_v1,
            list_objects_v2: config.list_objects_v2,
            delete_objects: config.delete_objects,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_maps_toggles() {
        let config = tinio_config::s3::Config {
            copy_object: false,
            multipart: false,
            ..Default::default()
        };
        let caps = Capabilities::from(config);
        assert!(!caps.copy_object);
        assert!(!caps.multipart);
        assert!(caps.delete_objects);
        assert!(caps.list_objects_v1);
        assert!(caps.list_objects_v2);
    }
}
