use std::path::PathBuf;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::types::{EngineError, ErrorCategory, ErrorPhase, Result, StatusCode};

pub(crate) trait TransportStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> TransportStream for T {}
pub(crate) type BoxStream = Box<dyn TransportStream>;

pub fn resolve_transport_address(endpoint_name: &str, token: &str) -> Result<PathBuf> {
    if !valid_endpoint_name(endpoint_name) {
        return Err(configuration_error(
            "endpoint_name must match [A-Za-z0-9][A-Za-z0-9_-]{0,31}",
        ));
    }
    if !valid_token(token) {
        return Err(configuration_error(
            "token must contain exactly 32 lowercase hexadecimal characters",
        ));
    }
    let stem = format!("yuumi-{endpoint_name}-{token}");
    #[cfg(windows)]
    let address = PathBuf::from(format!(r"\\.\pipe\{stem}"));
    #[cfg(unix)]
    let address = std::env::temp_dir().join(format!("{stem}.sock"));
    #[cfg(unix)]
    validate_unix_address(&address)?;
    Ok(address)
}

fn valid_endpoint_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=32).contains(&bytes.len())
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn valid_token(value: &str) -> bool {
    value.len() == 32
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

fn configuration_error(cause: impl Into<String>) -> EngineError {
    EngineError::new(
        ErrorCategory::Configuration,
        StatusCode::ErrProtocolViolation,
        ErrorPhase::Configuration,
        cause,
        None,
    )
}

#[cfg(unix)]
fn validate_unix_address(address: &std::path::Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    #[cfg(target_os = "macos")]
    const SUN_PATH_BYTES: usize = 104;
    #[cfg(not(target_os = "macos"))]
    const SUN_PATH_BYTES: usize = 108;
    if address.as_os_str().as_bytes().len() >= SUN_PATH_BYTES {
        return Err(configuration_error(
            "canonical Unix socket address exceeds the platform bound",
        ));
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) struct PlatformListener {
    inner: tokio::net::UnixListener,
}

#[cfg(unix)]
impl PlatformListener {
    pub(crate) async fn open(address: &std::path::Path) -> Result<Self> {
        use std::io::ErrorKind;
        use std::os::unix::fs::PermissionsExt;

        match tokio::net::UnixStream::connect(address).await {
            Ok(_) => {
                return Err(endpoint_error(
                    "endpoint is already owned by a live listener",
                ))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
                match std::fs::remove_file(address) {
                    Ok(()) => {}
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(endpoint_error(format!(
                            "stale endpoint removal failed: {error}"
                        )))
                    }
                }
            }
            Err(error) => return Err(endpoint_error(format!("endpoint probe failed: {error}"))),
        }
        let inner = tokio::net::UnixListener::bind(address)
            .map_err(|error| endpoint_error(format!("endpoint open failed: {error}")))?;
        let permissions = std::fs::Permissions::from_mode(0o600);
        if let Err(error) = std::fs::set_permissions(address, permissions) {
            drop(inner);
            let _ = std::fs::remove_file(address);
            return Err(endpoint_error(format!(
                "socket permission setup failed: {error}"
            )));
        }
        Ok(Self { inner })
    }

    pub(crate) async fn accept(&self) -> std::io::Result<(BoxStream, Option<u32>)> {
        let (stream, _) = self.inner.accept().await?;
        let peer_pid = stream
            .peer_cred()
            .ok()
            .and_then(|credentials| credentials.pid())
            .and_then(|pid| u32::try_from(pid).ok());
        Ok((Box::new(stream), peer_pid))
    }
}

#[cfg(windows)]
pub(crate) struct PlatformListener {
    address: PathBuf,
    pending: tokio::sync::Mutex<Option<tokio::net::windows::named_pipe::NamedPipeServer>>,
}

#[cfg(windows)]
impl PlatformListener {
    pub(crate) async fn open(address: &std::path::Path) -> Result<Self> {
        use std::io::ErrorKind;
        use tokio::net::windows::named_pipe::ClientOptions;

        match ClientOptions::new().open(address) {
            Ok(_) => {
                return Err(endpoint_error(
                    "endpoint is already owned by a live listener",
                ))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) if error.raw_os_error() == Some(231) => {
                return Err(endpoint_error(
                    "endpoint is busy and owned by a live listener",
                ));
            }
            Err(error) => return Err(endpoint_error(format!("endpoint probe failed: {error}"))),
        }
        let first = create_pipe(address, true)
            .map_err(|error| endpoint_error(format!("endpoint open failed: {error}")))?;
        Ok(Self {
            address: address.to_path_buf(),
            pending: tokio::sync::Mutex::new(Some(first)),
        })
    }

    pub(crate) async fn accept(&self) -> std::io::Result<(BoxStream, Option<u32>)> {
        let mut pending = self.pending.lock().await;
        let server = pending.take().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "listener is closed")
        })?;
        if let Err(error) = server.connect().await {
            *pending = Some(create_pipe(&self.address, false)?);
            return Err(error);
        }
        let peer_pid = named_pipe_client_pid(&server);
        *pending = Some(create_pipe(&self.address, false)?);
        Ok((Box::new(server), peer_pid))
    }
}

fn endpoint_error(cause: impl Into<String>) -> EngineError {
    EngineError::new(
        ErrorCategory::Endpoint,
        StatusCode::ErrPipeFailed,
        ErrorPhase::EndpointOpen,
        cause,
        None,
    )
}

#[cfg(windows)]
fn create_pipe(
    address: &std::path::Path,
    first: bool,
) -> std::io::Result<tokio::net::windows::named_pipe::NamedPipeServer> {
    use tokio::net::windows::named_pipe::{PipeMode, ServerOptions};
    let mut options = ServerOptions::new();
    options
        .pipe_mode(PipeMode::Byte)
        .reject_remote_clients(true)
        .first_pipe_instance(first);
    let mut security = windows_security::SecurityAttributes::current_user()?;
    unsafe { options.create_with_security_attributes_raw(address, security.as_raw()) }
}

#[cfg(windows)]
fn named_pipe_client_pid(server: &tokio::net::windows::named_pipe::NamedPipeServer) -> Option<u32> {
    use std::os::windows::io::AsRawHandle;
    let mut pid = 0u32;
    let ok = unsafe { GetNamedPipeClientProcessId(server.as_raw_handle(), &mut pid) };
    (ok != 0).then_some(pid)
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetNamedPipeClientProcessId(pipe: std::os::windows::io::RawHandle, pid: *mut u32) -> i32;
}

#[cfg(windows)]
mod windows_security {
    use std::ffi::c_void;
    use std::ptr::null_mut;

    const TOKEN_QUERY: u32 = 0x0008;
    const TOKEN_USER: u32 = 1;
    const SDDL_REVISION_1: u32 = 1;

    #[repr(C)]
    struct SecurityAttributesRaw {
        length: u32,
        security_descriptor: *mut c_void,
        inherit_handle: i32,
    }

    pub(super) struct SecurityAttributes {
        raw: SecurityAttributesRaw,
    }

    impl SecurityAttributes {
        pub(super) fn current_user() -> std::io::Result<Self> {
            let sid = current_user_sid()?;
            let sddl = format!("D:P(A;;GA;;;SY)(A;;GA;;;{sid})");
            let wide: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
            let mut descriptor = null_mut();
            let ok = unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    wide.as_ptr(),
                    SDDL_REVISION_1,
                    &mut descriptor,
                    null_mut(),
                )
            };
            if ok == 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(Self {
                raw: SecurityAttributesRaw {
                    length: std::mem::size_of::<SecurityAttributesRaw>() as u32,
                    security_descriptor: descriptor,
                    inherit_handle: 0,
                },
            })
        }

        pub(super) fn as_raw(&mut self) -> *mut c_void {
            (&mut self.raw as *mut SecurityAttributesRaw).cast()
        }
    }

    impl Drop for SecurityAttributes {
        fn drop(&mut self) {
            unsafe { LocalFree(self.raw.security_descriptor) };
        }
    }

    fn current_user_sid() -> std::io::Result<String> {
        let mut token = null_mut();
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let result = (|| {
            let mut size = 0u32;
            unsafe { GetTokenInformation(token, TOKEN_USER, null_mut(), 0, &mut size) };
            if size == 0 {
                return Err(std::io::Error::last_os_error());
            }
            let words = (size as usize).div_ceil(std::mem::size_of::<usize>());
            let mut buffer = vec![0usize; words];
            if unsafe {
                GetTokenInformation(
                    token,
                    TOKEN_USER,
                    buffer.as_mut_ptr().cast(),
                    size,
                    &mut size,
                )
            } == 0
            {
                return Err(std::io::Error::last_os_error());
            }
            let sid = unsafe { *(buffer.as_ptr() as *const *mut c_void) };
            let mut text = null_mut();
            if unsafe { ConvertSidToStringSidW(sid, &mut text) } == 0 {
                return Err(std::io::Error::last_os_error());
            }
            let length = (0..)
                .take_while(|&index| unsafe { *text.add(index) } != 0)
                .count();
            let value =
                String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(text, length) });
            unsafe { LocalFree(text.cast()) };
            Ok(value)
        })();
        unsafe { CloseHandle(token) };
        result
    }

    #[link(name = "advapi32")]
    unsafe extern "system" {
        fn OpenProcessToken(process: *mut c_void, access: u32, token: *mut *mut c_void) -> i32;
        fn GetTokenInformation(
            token: *mut c_void,
            class: u32,
            info: *mut c_void,
            length: u32,
            returned: *mut u32,
        ) -> i32;
        fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
            text: *const u16,
            revision: u32,
            descriptor: *mut *mut c_void,
            size: *mut u32,
        ) -> i32;
        fn ConvertSidToStringSidW(sid: *mut c_void, text: *mut *mut u16) -> i32;
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn CloseHandle(handle: *mut c_void) -> i32;
        fn LocalFree(memory: *mut c_void) -> *mut c_void;
    }
}
