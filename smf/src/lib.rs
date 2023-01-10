// Copyright 2021 Oxide Computer Company

use num_traits::FromPrimitive;
use std::env;
use std::ffi::{CStr, CString};
use std::ptr::{self, NonNull};
use thiserror::Error;

#[macro_use]
extern crate num_derive;

mod scf_sys;
pub use scf_sys::scf_type_t;
use scf_sys::*;

mod scope;
pub use scope::{Scope, Scopes};

mod service;
pub use service::{Service, Services};

mod instance;
pub use instance::{Instance, Instances};

mod snapshot;
pub use snapshot::{Snapshot, Snapshots};

mod propertygroup;
pub use propertygroup::{PropertyGroup, PropertyGroups};

mod property;
pub use property::{Properties, Property};

mod value;
pub use value::{Value, Values};

mod transaction;
pub use transaction::{CommitResult, Transaction};

pub type Result<T> = std::result::Result<T, ScfError>;

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
    #[error("environment variable SMF_FMRI not found")]
    MissingSmfFmriEnvironmentVariable,
    #[error("unknown error ({0})")]
    Unknown(u32),
}

impl From<u32> for ScfError {
    fn from(error: u32) -> Self {
        use scf_error_t::*;
        use ScfError::*;

        match scf_error_t::from_u32(error) {
            Some(SCF_ERROR_NONE) => None,
            Some(SCF_ERROR_NOT_BOUND) => NotBound,
            Some(SCF_ERROR_NOT_SET) => NotSet,
            Some(SCF_ERROR_NOT_FOUND) => NotFound,
            Some(SCF_ERROR_TYPE_MISMATCH) => TypeMismatch,
            Some(SCF_ERROR_IN_USE) => InUse,
            Some(SCF_ERROR_CONNECTION_BROKEN) => ConnectionBroken,
            Some(SCF_ERROR_INVALID_ARGUMENT) => InvalidArgument,
            Some(SCF_ERROR_NO_MEMORY) => NoMemory,
            Some(SCF_ERROR_CONSTRAINT_VIOLATED) => ConstraintViolated,
            Some(SCF_ERROR_EXISTS) => Exists,
            Some(SCF_ERROR_NO_SERVER) => NoServer,
            Some(SCF_ERROR_NO_RESOURCES) => NoResources,
            Some(SCF_ERROR_PERMISSION_DENIED) => PermissionDenied,
            Some(SCF_ERROR_BACKEND_ACCESS) => BackendAccess,
            Some(SCF_ERROR_HANDLE_MISMATCH) => HandleMismatch,
            Some(SCF_ERROR_HANDLE_DESTROYED) => HandleDestroyed,
            Some(SCF_ERROR_VERSION_MISMATCH) => VersionMismatch,
            Some(SCF_ERROR_BACKEND_READONLY) => BackendReadonly,
            Some(SCF_ERROR_DELETED) => Deleted,
            Some(SCF_ERROR_TEMPLATE_INVALID) => TemplateInvalid,
            Some(SCF_ERROR_CALLBACK_FAILED) => CallbackFailed,
            Some(SCF_ERROR_INTERNAL) => Internal,
            Option::None => Unknown(error),
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

    pub fn get_instance_from_fmri(&self, fmri: &str) -> Result<Instance<'_>> {
        let fmri = CString::new(fmri).unwrap();

        let scope = Scope::new(self)?;
        let service = Service::new(self)?;
        let instance = Instance::new(self)?;

        let ret = unsafe {
            scf_handle_decode_fmri(
                self.handle.as_ptr(),
                fmri.as_ptr(),
                scope.scope.as_ptr(),
                service.service.as_ptr(),
                instance.instance.as_ptr(),
                ptr::null_mut(),
                ptr::null_mut(),
                // `fmri` _must_ have an instance, but is allowed to also
                // specify a propertygroup / property; ignore those if present
                // via `SCF_DECODE_FMRI_TRUNCATE`.
                SCF_DECODE_FMRI_REQUIRE_INSTANCE | SCF_DECODE_FMRI_TRUNCATE,
            )
        };

        if ret == 0 {
            Ok(instance)
        } else {
            Err(ScfError::last())
        }
    }

    /// From within a running service instance, get our own [`Instance`].
    ///
    /// If you are using this to look up the current value of your properties,
    /// you almost certainly want [`get_self_snapshot()`] instead!
    ///
    /// This method looks up our own FMRI via the `SMF_FMRI` environment
    /// variable, which is supplied by `smf` to running instances.
    pub fn get_self_instance(&self) -> Result<Instance<'_>> {
        let fmri = env::var("SMF_FMRI")
            .map_err(|_| ScfError::MissingSmfFmriEnvironmentVariable)?;

        self.get_instance_from_fmri(&fmri)
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
    _scf: &'a Scf, // Prevent from being dropped
    iter: NonNull<scf_iter_t>,
}

impl<'a> Iter<'a> {
    fn new(scf: &'a Scf) -> Result<Iter> {
        if let Some(iter) =
            NonNull::new(unsafe { scf_iter_create(scf.handle.as_ptr()) })
        {
            Ok(Iter { _scf: scf, iter })
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

#[cfg(all(target_os = "illumos", test))]
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

    #[test]
    fn snapshot_iter() {
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

                let snapshots = i.snapshots().expect("snapshot iterator");
                for sn in snapshots {
                    println!("    {:?}", i);
                    let sn = sn.expect("snapshot");
                    println!("        {}", sn.name().expect("snapshot name"));
                }
            }
        }
    }
}
