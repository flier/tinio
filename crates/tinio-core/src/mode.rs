//! The write policy of a served plane (FR-023).

/// The write policy of a served plane: [`ReadWrite`](Self::ReadWrite) serves
/// every operation, [`ReadOnly`](Self::ReadOnly) refuses the mutating ones
/// with `AccessDenied`.
///
/// The mode is carried independently by the three option structs that act
/// on it — the fs options, the cleanup options and the scanner options —
/// and nothing cross-checks them: a caller must derive all three from one
/// value, or a read-only storage can still be handed a read-write repair
/// or scan.
///
/// # Examples
///
/// ```rust
/// use tinio_core::mode::AccessMode;
///
/// assert!(!AccessMode::default().is_read_only());
/// assert!(AccessMode::from(true).is_read_only());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AccessMode {
    /// Every operation is served.
    #[default]
    ReadWrite,
    /// Mutating operations answer `AccessDenied` (FR-023).
    ReadOnly,
}

impl AccessMode {
    /// Whether this mode refuses mutating operations.
    pub fn is_read_only(self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

impl From<bool> for AccessMode {
    fn from(read_only: bool) -> Self {
        if read_only {
            Self::ReadOnly
        } else {
            Self::ReadWrite
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_read_write() {
        assert_eq!(AccessMode::default(), AccessMode::ReadWrite);
        assert!(!AccessMode::default().is_read_only());
    }

    #[test]
    fn bool_maps_to_mode() {
        assert_eq!(AccessMode::from(true), AccessMode::ReadOnly);
        assert_eq!(AccessMode::from(false), AccessMode::ReadWrite);
        assert!(AccessMode::from(true).is_read_only());
    }
}
