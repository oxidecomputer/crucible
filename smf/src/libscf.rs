#![allow(non_camel_case_types)]
#![allow(dead_code)]

use libc::{size_t, ssize_t};
use std::os::raw::{c_char, c_int, c_ulong};

type scf_version_t = c_ulong;

pub const SCF_VERSION: scf_version_t = 1;

macro_rules! opaque_handle {
    ($type_name:ident) => {
        pub enum $type_name {}
        impl Copy for $type_name {}
        impl Clone for $type_name {
            fn clone(&self) -> $type_name {
                *self
            }
        }
    };
}

opaque_handle!(scf_handle_t);
opaque_handle!(scf_iter_t);
opaque_handle!(scf_scope_t);
opaque_handle!(scf_service_t);
opaque_handle!(scf_instance_t);

#[derive(Debug)]
#[repr(C)]
pub enum scf_error_t {
    SCF_ERROR_NONE = 1000,            /* no error */
    SCF_ERROR_NOT_BOUND,              /* handle not bound */
    SCF_ERROR_NOT_SET,                /* cannot use unset argument */
    SCF_ERROR_NOT_FOUND,              /* nothing of that name found */
    SCF_ERROR_TYPE_MISMATCH,          /* type does not match value */
    SCF_ERROR_IN_USE,                 /* cannot modify while in-use */
    SCF_ERROR_CONNECTION_BROKEN,      /* repository connection gone */
    SCF_ERROR_INVALID_ARGUMENT,       /* bad argument */
    SCF_ERROR_NO_MEMORY,              /* no memory available */
    SCF_ERROR_CONSTRAINT_VIOLATED,    /* required constraint not met */
    SCF_ERROR_EXISTS,                 /* object already exists */
    SCF_ERROR_NO_SERVER,              /* repository server unavailable */
    SCF_ERROR_NO_RESOURCES,           /* server has insufficient resources */
    SCF_ERROR_PERMISSION_DENIED,      /* insufficient privileges for action */
    SCF_ERROR_BACKEND_ACCESS,         /* backend refused access */
    SCF_ERROR_HANDLE_MISMATCH,        /* mismatched SCF handles */
    SCF_ERROR_HANDLE_DESTROYED,       /* object bound to destroyed handle */
    SCF_ERROR_VERSION_MISMATCH,       /* incompatible SCF version */
    SCF_ERROR_BACKEND_READONLY,       /* backend is read-only */
    SCF_ERROR_DELETED,                /* object has been deleted */
    SCF_ERROR_TEMPLATE_INVALID,       /* template data is invalid */
    SCF_ERROR_CALLBACK_FAILED = 1080, /* user callback function failed */
    SCF_ERROR_INTERNAL = 1101,        /* internal error */
}

pub const SCF_LIMIT_MAX_NAME_LENGTH: u32 = 0xfffff830;
pub const SCF_LIMIT_MAX_VALUE_LENGTH: u32 = 0xfffff82f;
pub const SCF_LIMIT_MAX_PG_TYPE_LENGTH: u32 = 0xfffff82e;
pub const SCF_LIMIT_MAX_FMRI_LENGTH: u32 = 0xfffff82d;

pub const SCF_SCOPE_LOCAL: &[u8] = b"localhost\0";

#[link(name = "scf")]
extern "C" {
    pub fn scf_handle_create(version: scf_version_t) -> *mut scf_handle_t;
    pub fn scf_handle_destroy(handle: *mut scf_handle_t);
    pub fn scf_handle_bind(handle: *mut scf_handle_t) -> c_int;
    pub fn scf_handle_unbind(handle: *mut scf_handle_t) -> c_int;

    pub fn scf_myname(
        handle: *mut scf_handle_t,
        out: *mut c_char,
        sz: size_t,
    ) -> ssize_t;

    pub fn scf_error() -> scf_error_t; /* XXX what about unknown values sigh */
    pub fn scf_strerror(error: scf_error_t) -> *const c_char;

    pub fn scf_limit(name: u32) -> ssize_t;

    pub fn scf_iter_create(handle: *mut scf_handle_t) -> *mut scf_iter_t;
    pub fn scf_iter_destroy(iter: *mut scf_iter_t);
    pub fn scf_iter_reset(iter: *mut scf_iter_t);

    pub fn scf_scope_create(handle: *mut scf_handle_t) -> *mut scf_scope_t;
    pub fn scf_scope_destroy(scope: *mut scf_scope_t);
    pub fn scf_scope_get_name(
        scope: *mut scf_scope_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t;
    pub fn scf_handle_get_scope(
        handle: *mut scf_handle_t,
        name: *const c_char,
        out: *mut scf_scope_t,
    ) -> c_int;

    pub fn scf_iter_handle_scopes(
        iter: *mut scf_iter_t,
        handle: *const scf_handle_t,
    ) -> c_int;
    pub fn scf_iter_next_scope(
        iter: *mut scf_iter_t,
        out: *mut scf_scope_t,
    ) -> c_int;

    pub fn scf_service_create(handle: *mut scf_handle_t) -> *mut scf_service_t;
    pub fn scf_service_destroy(service: *mut scf_service_t);

    pub fn scf_service_get_name(
        service: *mut scf_service_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t;

    pub fn scf_iter_scope_services(
        iter: *mut scf_iter_t,
        scope: *const scf_scope_t,
    ) -> c_int;
    pub fn scf_iter_next_service(
        iter: *mut scf_iter_t,
        out: *mut scf_service_t,
    ) -> c_int;
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! limit_test {
        ($limit:ident) => {
            let val = unsafe { scf_limit($limit) };
            println!("{} = {}", stringify!($limit), val);
            assert!(val > 0);
        };
    }

    #[test]
    fn limits() {
        limit_test!(SCF_LIMIT_MAX_NAME_LENGTH);
        limit_test!(SCF_LIMIT_MAX_VALUE_LENGTH);
        limit_test!(SCF_LIMIT_MAX_PG_TYPE_LENGTH);
        limit_test!(SCF_LIMIT_MAX_FMRI_LENGTH);
    }

    #[test]
    fn handle() {
        let handle = unsafe { scf_handle_create(SCF_VERSION) };
        assert!(!handle.is_null());
        unsafe { scf_handle_destroy(handle) };
    }
}
