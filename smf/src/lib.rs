use std::ffi::CStr;
use std::ptr::NonNull;
use thiserror::Error;

mod libscf;
use libscf::*;

mod service;
pub use service::{Service, Services};

mod instance;
pub use instance::{Instance, Instances};

mod scope;
pub use scope::{Scope, Scopes};

type Result<T> = std::result::Result<T, ScfError>;

#[derive(Error, Debug)]
pub enum ScfError {
    #[error("no error")]
    None,
    #[error("handle not bound")]
    NotBound,
    #[error("cannot use unset argument")]
    NotSet,
    #[error("nothing of that name found")]
    NotFound,
    #[error("type does not match value")]
    TypeMismatch,
    #[error("cannot modify while in-use")]
    InUse,
    #[error("repository connection gone")]
    ConnectionBroken,
    #[error("bad argument")]
    InvalidArgument,
    #[error("no memory available")]
    NoMemory,
    #[error("required constraint not met")]
    ConstraintViolated,
    #[error("object already exists")]
    Exists,
    #[error("repository server unavailable")]
    NoServer,
    #[error("server has insufficient resources")]
    NoResources,
    #[error("insufficient privileges for action")]
    PermissionDenied,
    #[error("backend refused access")]
    BackendAccess,
    #[error("mismatched SCF handles")]
    HandleMismatch,
    #[error("object bound to destroyed handle")]
    HandleDestroyed,
    #[error("incompatible SCF version")]
    VersionMismatch,
    #[error("backend is read-only")]
    BackendReadonly,
    #[error("object has been deleted")]
    Deleted,
    #[error("template data is invalid")]
    TemplateInvalid,
    #[error("user callback function failed")]
    CallbackFailed,
    #[error("internal error")]
    Internal,
    #[error("unknown error ({0})")]
    Unknown(u32),
}

impl From<scf_error_t> for ScfError {
    fn from(error: scf_error_t) -> Self {
        use scf_error_t::*;
        use ScfError::*;

        match error {
            SCF_ERROR_NONE => None,
            SCF_ERROR_NOT_BOUND => NotBound,
            SCF_ERROR_NOT_SET => NotSet,
            SCF_ERROR_NOT_FOUND => NotFound,
            SCF_ERROR_TYPE_MISMATCH => TypeMismatch,
            SCF_ERROR_IN_USE => InUse,
            SCF_ERROR_CONNECTION_BROKEN => ConnectionBroken,
            SCF_ERROR_INVALID_ARGUMENT => InvalidArgument,
            SCF_ERROR_NO_MEMORY => NoMemory,
            SCF_ERROR_CONSTRAINT_VIOLATED => ConstraintViolated,
            SCF_ERROR_EXISTS => Exists,
            SCF_ERROR_NO_SERVER => NoServer,
            SCF_ERROR_NO_RESOURCES => NoResources,
            SCF_ERROR_PERMISSION_DENIED => PermissionDenied,
            SCF_ERROR_BACKEND_ACCESS => BackendAccess,
            SCF_ERROR_HANDLE_MISMATCH => HandleMismatch,
            SCF_ERROR_HANDLE_DESTROYED => HandleDestroyed,
            SCF_ERROR_VERSION_MISMATCH => VersionMismatch,
            SCF_ERROR_BACKEND_READONLY => BackendReadonly,
            SCF_ERROR_DELETED => Deleted,
            SCF_ERROR_TEMPLATE_INVALID => TemplateInvalid,
            SCF_ERROR_CALLBACK_FAILED => CallbackFailed,
            SCF_ERROR_INTERNAL => Internal,
            unk => Unknown(unk as u32),
        }
    }
}

impl ScfError {
    fn last() -> Self {
        ScfError::from(unsafe { scf_error() })
    }
}

#[derive(Debug)]
pub struct Scf {
    handle: NonNull<scf_handle_t>,
}

impl Scf {
    pub fn new() -> Result<Scf> {
        if let Some(handle) =
            NonNull::new(unsafe { scf_handle_create(SCF_VERSION) })
        {
            if unsafe { scf_handle_bind(handle.as_ptr()) } != 0 {
                let e = ScfError::last();
                unsafe { scf_handle_destroy(handle.as_ptr()) };
                Err(e)
            } else {
                Ok(Scf { handle })
            }
        } else {
            Err(ScfError::last())
        }
    }

    pub fn scope_local(&self) -> Result<Scope> {
        let scope = Scope::new(self)?;

        if unsafe {
            scf_handle_get_scope(
                self.handle.as_ptr(),
                SCF_SCOPE_LOCAL.as_ptr() as *mut libc::c_char,
                scope.scope.as_ptr(),
            )
        } != 0
        {
            return Err(ScfError::last());
        }

        Ok(scope)
    }

    pub fn scopes(&self) -> Result<Scopes> {
        Scopes::new(self)
    }
}

impl Drop for Scf {
    fn drop(&mut self) {
        unsafe { scf_handle_unbind(self.handle.as_ptr()) };
        unsafe { scf_handle_destroy(self.handle.as_ptr()) };
    }
}

fn buf_for(name: u32) -> Result<Vec<u8>> {
    let len = unsafe { scf_limit(name) };
    if len < 0 {
        return Err(ScfError::last());
    }
    let len = len as usize;

    Ok(vec![0; len + 1])
}

fn str_from(buf: &mut Vec<u8>, ret: libc::ssize_t) -> Result<String> {
    if ret < 0 {
        return Err(ScfError::last());
    }
    let sz = ret as usize;
    unsafe { buf.set_len(sz + 1) };

    Ok(CStr::from_bytes_with_nul(buf)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string())
}

#[derive(Debug)]
pub struct Iter<'a> {
    scf: &'a Scf,
    iter: NonNull<scf_iter_t>,
}

impl<'a> Iter<'a> {
    fn new(scf: &'a Scf) -> Result<Iter> {
        if let Some(iter) =
            NonNull::new(unsafe { scf_iter_create(scf.handle.as_ptr()) })
        {
            Ok(Iter { scf, iter })
        } else {
            Err(ScfError::last())
        }
    }

    #[allow(dead_code)]
    fn reset(&self) {
        unsafe { scf_iter_reset(self.iter.as_ptr()) };
    }
}

impl Drop for Iter<'_> {
    fn drop(&mut self) {
        unsafe { scf_iter_destroy(self.iter.as_ptr()) };
    }
}

#[cfg(test)]
mod tests {
    use super::{Iter, Scf, ScfError, Scope};

    #[test]
    fn basic() {
        let _scf = Scf::new().expect("scf");
    }

    #[test]
    fn scope_not_set() {
        let scf = Scf::new().expect("scf");
        let scope = Scope::new(&scf).expect("scope");
        let name = scope.name();
        assert!(matches!(name, Err(ScfError::NotSet)));
    }

    #[test]
    fn scope_local() {
        let scf = Scf::new().expect("scf");
        let local = scf.scope_local().expect("scope local");
        let name = local.name();
        println!("name = {:?}", name);
        assert_eq!(name.unwrap().as_str(), "localhost");
    }

    #[test]
    fn iter() {
        let scf = Scf::new().expect("scf");
        let _iter = Iter::new(&scf).expect("iter");
    }

    #[test]
    fn scope_iter() {
        let scf = Scf::new().expect("scf");
        let scopes = scf.scopes().expect("scopes");
        let names = scopes.map(|s| s.map(|s| s.name())).collect::<Vec<_>>();
        assert_eq!(names.len(), 1);
        assert_eq!(
            names[0].as_ref().unwrap().as_ref().unwrap().as_str(),
            "localhost"
        );
    }

    #[test]
    fn service_iter() {
        let scf = Scf::new().expect("scf");
        let local = scf.scope_local().expect("local scope");
        let services = local.services().expect("service iterator");
        for s in services {
            println!("{:?}", s);
            let s = s.expect("service");
            println!("{}", s.name().expect("service name"));
        }
    }

    #[test]
    fn instance_iter() {
        let scf = Scf::new().expect("scf");
        let local = scf.scope_local().expect("local scope");
        let services = local.services().expect("service iterator");
        for s in services {
            println!("{:?}", s);
            let s = s.expect("service");
            println!("{}", s.name().expect("service name"));

            let instances = s.instances().expect("instance iterator");
            for i in instances {
                println!("    {:?}", i);
                let i = i.expect("instance");
                println!("    {}", i.name().expect("instance name"));
            }
        }
    }
}
