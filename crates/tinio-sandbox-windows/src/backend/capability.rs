//! The capability list the container may hold: one name an operator writes, one
//! SID the kernel is handed.
//!
//! The list is empty by default and that is the whole of the Windows network
//! guarantee (design §7), so this module is the two spellings and nothing else:
//! the name an operator writes — `TINIO_SANDBOX_CAPABILITIES`, or the
//! [`Options::capabilities`](super::Options::capabilities) a library caller
//! builds — and the `S-1-15-3-N` SID the container is created with —
//! [`Capability::sid`], the one text `sid::CapabilitySids` resolves.
//!
//! **The rationale did not move without the code it explains**: each variant's
//! meaning, the round trip between the two spellings and the refusal of an
//! unknown name are documented on the items below, exactly as they were before
//! the backend became a directory module.

use crate::_core::{Error, Result, WINDOWS_CAPABILITY_USAGE};

/// A capability the container may hold.
///
/// Empty by default, on purpose: the whole of the Windows network guarantee is
/// that a container with no network capability cannot open an outbound
/// connection (design §7). Capabilities are additive, so an operator who needs a
/// non-loopback listener adds
/// [`PrivateNetworkClientServer`](Self::PrivateNetworkClientServer) and accepts
/// that the container may then reach the private network.
///
/// **The enum is closed, and that is what bounds the container's capability
/// list**: every SID one of the three variants names is inside the AppContainer
/// capability range, and `sid::CapabilitySids` stamps what it resolves enabled —
/// so a signature that took SID text would be one a caller could hand
/// `S-1-5-32-544` (`BUILTIN\Administrators`) through. The three variants are the
/// whole of what this crate can put in that list.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::backend::Capability;
///
/// let capability = Capability::from_name("internetClient")?;
/// assert_eq!(capability, Capability::InternetClient);
/// assert_eq!(capability.sid(), "S-1-15-3-1");
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// `internetClient` — outbound to the internet.
    InternetClient,
    /// `internetClientServer` — outbound, and inbound from the internet.
    InternetClientServer,
    /// `privateNetworkClientServer` — inbound from the private network, which is
    /// what a non-loopback bind needs (design §7).
    PrivateNetworkClientServer,
}

impl Capability {
    /// The capability's SID.
    ///
    /// `S-1-15-3-N` is the AppContainer capability range: the name is the
    /// operator's spelling and this is the kernel's, and it is the one text
    /// `sid::CapabilitySids::resolve` turns into the entry a container is created
    /// with — which is what makes a group SID outside that range unspellable
    /// there.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::backend::Capability;
    ///
    /// assert_eq!(Capability::InternetClient.sid(), "S-1-15-3-1");
    /// assert_eq!(Capability::PrivateNetworkClientServer.sid(), "S-1-15-3-3");
    /// ```
    pub const fn sid(self) -> &'static str {
        match self {
            Self::InternetClient => "S-1-15-3-1",
            Self::InternetClientServer => "S-1-15-3-2",
            Self::PrivateNetworkClientServer => "S-1-15-3-3",
        }
    }

    /// The name `TINIO_SANDBOX_CAPABILITIES` accepts.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::backend::Capability;
    ///
    /// assert_eq!(Capability::InternetClient.name(), "internetClient");
    /// // Every name is one [`Capability::from_name`] accepts, which is what
    /// // makes the two spellings a round trip.
    /// let capability = Capability::from_name(Capability::InternetClient.name())?;
    /// assert_eq!(capability, Capability::InternetClient);
    /// # Ok::<(), tinio_sandbox_core::Error>(())
    /// ```
    pub const fn name(self) -> &'static str {
        match self {
            Self::InternetClient => "internetClient",
            Self::InternetClientServer => "internetClientServer",
            Self::PrivateNetworkClientServer => "privateNetworkClientServer",
        }
    }

    /// Parse a capability by name.
    ///
    /// # Errors
    ///
    /// [`Error::Usage`] for an unknown name, so a typo cannot silently
    /// leave the container with fewer capabilities than the operator asked for.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::backend::Capability;
    ///
    /// assert_eq!(
    ///     Capability::from_name("privateNetworkClientServer")?,
    ///     Capability::PrivateNetworkClientServer
    /// );
    /// assert!(Capability::from_name("bogus").is_err());
    /// # Ok::<(), tinio_sandbox_core::Error>(())
    /// ```
    pub fn from_name(name: &str) -> Result<Self> {
        match name {
            "internetClient" => Ok(Self::InternetClient),
            "internetClientServer" => Ok(Self::InternetClientServer),
            "privateNetworkClientServer" => Ok(Self::PrivateNetworkClientServer),
            _ => Err(Error::Usage {
                detail: WINDOWS_CAPABILITY_USAGE,
            }),
        }
    }

    /// Parse a comma-separated list, the form the environment channel uses.
    ///
    /// # Errors
    ///
    /// [`Error::Usage`] for any unknown entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::backend::Capability;
    ///
    /// // Absent and empty are both "no capability", which is the default the
    /// // outbound guarantee rests on (design §7).
    /// assert!(Capability::parse_list(None)?.is_empty());
    /// assert!(Capability::parse_list(Some(""))?.is_empty());
    /// assert_eq!(
    ///     Capability::parse_list(Some("internetClient, privateNetworkClientServer"))?.len(),
    ///     2
    /// );
    /// # Ok::<(), tinio_sandbox_core::Error>(())
    /// ```
    pub fn parse_list(raw: Option<&str>) -> Result<Vec<Self>> {
        let Some(raw) = raw else {
            return Ok(Vec::new());
        };
        raw.split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(Self::from_name)
            .collect()
    }
}
