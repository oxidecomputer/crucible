// Copyright 2021 Oxide Computer Company

#![allow(non_camel_case_types)]
#![allow(dead_code)]

use libc::{size_t, ssize_t};
use std::os::raw::{c_char, c_int, c_ulong};
use std::marker::{PhantomData, PhantomPinned};

type scf_version_t = c_ulong;

pub const SCF_VERSION: scf_version_t = 1;

pub const SCF_PG_FLAG_NONPERSISTENT: u32 = 0x1;

/*
 * Flags for the smf_enable_instance(3SCF) family of functions:
 */
pub const SMF_IMMEDIATE: c_int = 0x1;
pub const SMF_TEMPORARY: c_int = 0x2;
pub const SMF_AT_NEXT_BOOT: c_int = 0x4;

macro_rules! opaque_handle {
    ($type_name:ident) => {
        #[repr(C)]
        pub struct $type_name {
            _data: [u8; 0],
            // See https://doc.rust-lang.org/nomicon/ffi.html; this marker
            // guarantees our type does not implement `Send`, `Sync`, or
            // `Unpin`.
            _marker: PhantomData<(*mut u8, PhantomPinned)>,
        }
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
opaque_handle!(scf_snapshot_t);
opaque_handle!(scf_propertygroup_t);
opaque_handle!(scf_property_t);
opaque_handle!(scf_value_t);
opaque_handle!(scf_snaplevel_t);
opaque_handle!(scf_transaction_t);
opaque_handle!(scf_transaction_entry_t);

#[derive(Debug, FromPrimitive, ToPrimitive)]
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

#[derive(Debug, FromPrimitive, ToPrimitive, Clone, Copy)]
#[repr(C)]
pub enum scf_type_t {
    SCF_TYPE_INVALID = 0,

    SCF_TYPE_BOOLEAN,
    SCF_TYPE_COUNT,
    SCF_TYPE_INTEGER,
    SCF_TYPE_TIME,
    SCF_TYPE_ASTRING,
    SCF_TYPE_OPAQUE,

    SCF_TYPE_USTRING = 100,

    SCF_TYPE_URI = 200,
    SCF_TYPE_FMRI,

    SCF_TYPE_HOST = 300,
    SCF_TYPE_HOSTNAME,
    SCF_TYPE_NET_ADDR_V4,
    SCF_TYPE_NET_ADDR_V6,
    SCF_TYPE_NET_ADDR,
}

pub const SCF_LIMIT_MAX_NAME_LENGTH: u32 = 0xfffff830;
pub const SCF_LIMIT_MAX_VALUE_LENGTH: u32 = 0xfffff82f;
pub const SCF_LIMIT_MAX_PG_TYPE_LENGTH: u32 = 0xfffff82e;
pub const SCF_LIMIT_MAX_FMRI_LENGTH: u32 = 0xfffff82d;

pub const SCF_SCOPE_LOCAL: &[u8] = b"localhost\0";

#[cfg(target_os = "illumos")]
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

    pub fn scf_error() -> u32;
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

    pub fn scf_scope_get_service(
        scope: *mut scf_scope_t,
        name: *const c_char,
        out: *mut scf_service_t,
    ) -> c_int;
    pub fn scf_scope_add_service(
        scope: *mut scf_scope_t,
        name: *const c_char,
        out: *mut scf_service_t,
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
    pub fn scf_service_get_instance(
        service: *mut scf_service_t,
        name: *const c_char,
        out: *mut scf_instance_t,
    ) -> c_int;
    pub fn scf_service_add_instance(
        service: *mut scf_service_t,
        name: *const c_char,
        out: *mut scf_instance_t,
    ) -> c_int;

    pub fn scf_iter_scope_services(
        iter: *mut scf_iter_t,
        scope: *const scf_scope_t,
    ) -> c_int;
    pub fn scf_iter_next_service(
        iter: *mut scf_iter_t,
        out: *mut scf_service_t,
    ) -> c_int;

    pub fn scf_instance_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_instance_t;
    pub fn scf_instance_destroy(instance: *mut scf_instance_t);

    pub fn scf_instance_get_name(
        instance: *mut scf_instance_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t;
    pub fn scf_instance_get_pg(
        instance: *mut scf_instance_t,
        name: *const c_char,
        out: *mut scf_propertygroup_t,
    ) -> c_int;
    pub fn scf_instance_add_pg(
        instance: *mut scf_instance_t,
        name: *const c_char,
        pgtype: *const c_char,
        flags: u32,
        out: *mut scf_propertygroup_t,
    ) -> c_int;
    pub fn scf_instance_get_pg_composed(
        instance: *mut scf_instance_t,
        snapshot: *mut scf_snapshot_t,
        name: *const c_char,
        out: *mut scf_propertygroup_t,
    ) -> c_int;
    pub fn scf_instance_get_snapshot(
        instance: *mut scf_instance_t,
        name: *const c_char,
        out: *mut scf_snapshot_t,
    ) -> c_int;
    pub fn scf_instance_to_fmri(
        instance: *const scf_instance_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t;

    pub fn scf_iter_service_instances(
        iter: *mut scf_iter_t,
        service: *const scf_service_t,
    ) -> c_int;
    pub fn scf_iter_next_instance(
        iter: *mut scf_iter_t,
        out: *mut scf_instance_t,
    ) -> c_int;

    pub fn scf_snapshot_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_snapshot_t;
    pub fn scf_snapshot_destroy(snapshot: *mut scf_snapshot_t);

    pub fn scf_snapshot_get_name(
        snapshot: *mut scf_snapshot_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t;
    pub fn scf_snapshot_get_parent(
        snapshot: *const scf_snapshot_t,
        inst: *mut scf_instance_t,
    ) -> c_int;

    pub fn scf_iter_instance_snapshots(
        iter: *mut scf_iter_t,
        instance: *const scf_instance_t,
    ) -> c_int;
    pub fn scf_iter_next_snapshot(
        iter: *mut scf_iter_t,
        out: *mut scf_snapshot_t,
    ) -> c_int;

    pub fn scf_iter_instance_pgs(
        iter: *mut scf_iter_t,
        instance: *const scf_instance_t,
    ) -> c_int;
    pub fn scf_iter_instance_pgs_composed(
        iter: *mut scf_iter_t,
        instance: *const scf_instance_t,
        snapshot: *const scf_snapshot_t,
    ) -> c_int;
    pub fn scf_iter_service_pgs(
        iter: *mut scf_iter_t,
        service: *const scf_service_t,
    ) -> c_int;
    pub fn scf_iter_next_pg(
        iter: *mut scf_iter_t,
        out: *mut scf_propertygroup_t,
    ) -> c_int;

    pub fn scf_pg_create(handle: *mut scf_handle_t)
        -> *mut scf_propertygroup_t;
    pub fn scf_pg_destroy(pg: *mut scf_propertygroup_t);

    pub fn scf_pg_get_name(
        pg: *mut scf_propertygroup_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t;
    pub fn scf_pg_get_type(
        pg: *mut scf_propertygroup_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t;
    pub fn scf_pg_get_flags(
        pg: *mut scf_propertygroup_t,
        out: *mut u32,
    ) -> c_int;
    pub fn scf_pg_update(pg: *mut scf_propertygroup_t) -> c_int;
    pub fn scf_pg_get_property(
        pg: *mut scf_propertygroup_t,
        name: *const c_char,
        out: *mut scf_property_t,
    ) -> c_int;

    pub fn scf_iter_pg_properties(
        iter: *mut scf_iter_t,
        pg: *const scf_propertygroup_t,
    ) -> c_int;
    pub fn scf_iter_next_property(
        iter: *mut scf_iter_t,
        out: *mut scf_property_t,
    ) -> c_int;

    pub fn scf_property_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_property_t;
    pub fn scf_property_destroy(prop: *mut scf_property_t);

    pub fn scf_property_get_name(
        prop: *mut scf_property_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t;
    pub fn scf_property_type(
        prop: *mut scf_property_t,
        typ: *mut scf_type_t,
    ) -> ssize_t;

    pub fn scf_iter_property_values(
        iter: *mut scf_iter_t,
        prop: *const scf_property_t,
    ) -> c_int;
    pub fn scf_iter_next_value(
        iter: *mut scf_iter_t,
        out: *mut scf_value_t,
    ) -> c_int;

    pub fn scf_value_create(handle: *mut scf_handle_t) -> *mut scf_value_t;
    pub fn scf_value_destroy(val: *mut scf_value_t);
    pub fn scf_value_reset(val: *mut scf_value_t);

    pub fn scf_value_type(val: *mut scf_value_t) -> c_int;
    pub fn scf_value_base_type(val: *mut scf_value_t) -> c_int;

    pub fn scf_value_get_as_string(
        val: *mut scf_value_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t;
    pub fn scf_value_set_from_string(
        val: *mut scf_value_t,
        valtype: scf_type_t,
        valstr: *const c_char,
    ) -> c_int;

    pub fn scf_type_base_type(typ: scf_type_t, out: *mut scf_type_t) -> c_int;

    pub fn scf_transaction_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_transaction_t;
    pub fn scf_transaction_destroy(tran: *mut scf_transaction_t);
    pub fn scf_transaction_reset(tran: *mut scf_transaction_t);
    pub fn scf_transaction_reset_all(tran: *mut scf_transaction_t);

    pub fn scf_transaction_start(
        tran: *mut scf_transaction_t,
        pg: *mut scf_propertygroup_t,
    ) -> c_int;
    pub fn scf_transaction_commit(tran: *mut scf_transaction_t) -> c_int;

    pub fn scf_transaction_property_delete(
        tran: *mut scf_transaction_t,
        entry: *mut scf_transaction_entry_t,
        name: *const c_char,
    ) -> c_int;
    pub fn scf_transaction_property_new(
        tran: *mut scf_transaction_t,
        entry: *mut scf_transaction_entry_t,
        name: *const c_char,
        proptype: scf_type_t,
    ) -> c_int;
    pub fn scf_transaction_property_change(
        tran: *mut scf_transaction_t,
        entry: *mut scf_transaction_entry_t,
        name: *const c_char,
        proptype: scf_type_t,
    ) -> c_int;
    pub fn scf_transaction_property_change_type(
        tran: *mut scf_transaction_t,
        entry: *mut scf_transaction_entry_t,
        name: *const c_char,
        proptype: scf_type_t,
    ) -> c_int;

    pub fn scf_entry_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_transaction_entry_t;
    pub fn scf_entry_destroy(tran: *mut scf_transaction_entry_t);
    pub fn scf_entry_destroy_children(tran: *mut scf_transaction_entry_t);
    pub fn scf_entry_reset(tran: *mut scf_transaction_entry_t);

    pub fn scf_entry_add_value(
        tran: *mut scf_transaction_entry_t,
        value: *mut scf_value_t,
    ) -> c_int;

    pub fn smf_disable_instance(instance: *const c_char, flags: c_int)
        -> c_int;
    pub fn smf_enable_instance(instance: *const c_char, flags: c_int) -> c_int;
}

#[cfg(not(target_os = "illumos"))]
mod dummy {
    #![allow(unused_variables)]

    use super::*;

    pub unsafe fn scf_handle_create(
        version: scf_version_t,
    ) -> *mut scf_handle_t {
        unimplemented!()
    }
    pub unsafe fn scf_handle_destroy(handle: *mut scf_handle_t) {
        unimplemented!()
    }
    pub unsafe fn scf_handle_bind(handle: *mut scf_handle_t) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_handle_unbind(handle: *mut scf_handle_t) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_myname(
        handle: *mut scf_handle_t,
        out: *mut c_char,
        sz: size_t,
    ) -> ssize_t {
        unimplemented!()
    }

    pub unsafe fn scf_error() -> u32 {
        unimplemented!()
    }
    pub unsafe fn scf_strerror(error: scf_error_t) -> *const c_char {
        unimplemented!()
    }

    pub unsafe fn scf_limit(name: u32) -> ssize_t {
        unimplemented!()
    }

    pub unsafe fn scf_iter_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_iter_t {
        unimplemented!()
    }
    pub unsafe fn scf_iter_destroy(iter: *mut scf_iter_t) {
        unimplemented!()
    }
    pub unsafe fn scf_iter_reset(iter: *mut scf_iter_t) {
        unimplemented!()
    }

    pub unsafe fn scf_scope_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_scope_t {
        unimplemented!()
    }
    pub unsafe fn scf_scope_destroy(scope: *mut scf_scope_t) {
        unimplemented!()
    }
    pub unsafe fn scf_scope_get_name(
        scope: *mut scf_scope_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t {
        unimplemented!()
    }
    pub unsafe fn scf_handle_get_scope(
        handle: *mut scf_handle_t,
        name: *const c_char,
        out: *mut scf_scope_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_scope_get_service(
        scope: *mut scf_scope_t,
        name: *const c_char,
        out: *mut scf_service_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_scope_add_service(
        scope: *mut scf_scope_t,
        name: *const c_char,
        out: *mut scf_service_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_iter_handle_scopes(
        iter: *mut scf_iter_t,
        handle: *const scf_handle_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_iter_next_scope(
        iter: *mut scf_iter_t,
        out: *mut scf_scope_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_service_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_service_t {
        unimplemented!()
    }
    pub unsafe fn scf_service_destroy(service: *mut scf_service_t) {
        unimplemented!()
    }

    pub unsafe fn scf_service_get_name(
        service: *mut scf_service_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t {
        unimplemented!()
    }
    pub unsafe fn scf_service_get_instance(
        service: *mut scf_service_t,
        name: *const c_char,
        out: *mut scf_instance_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_service_add_instance(
        service: *mut scf_service_t,
        name: *const c_char,
        out: *mut scf_instance_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_iter_scope_services(
        iter: *mut scf_iter_t,
        scope: *const scf_scope_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_iter_next_service(
        iter: *mut scf_iter_t,
        out: *mut scf_service_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_instance_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_instance_t {
        unimplemented!()
    }
    pub unsafe fn scf_instance_destroy(instance: *mut scf_instance_t) {
        unimplemented!()
    }

    pub unsafe fn scf_instance_get_name(
        instance: *mut scf_instance_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t {
        unimplemented!()
    }
    pub unsafe fn scf_instance_get_pg(
        instance: *mut scf_instance_t,
        name: *const c_char,
        out: *mut scf_propertygroup_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_instance_add_pg(
        instance: *mut scf_instance_t,
        name: *const c_char,
        pgtype: *const c_char,
        flags: u32,
        out: *mut scf_propertygroup_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_instance_get_pg_composed(
        instance: *mut scf_instance_t,
        snapshot: *mut scf_snapshot_t,
        name: *const c_char,
        out: *mut scf_propertygroup_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_instance_get_snapshot(
        instance: *mut scf_instance_t,
        name: *const c_char,
        out: *mut scf_snapshot_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_instance_to_fmri(
        instance: *const scf_instance_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t {
        unimplemented!()
    }

    pub unsafe fn scf_iter_service_instances(
        iter: *mut scf_iter_t,
        service: *const scf_service_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_iter_next_instance(
        iter: *mut scf_iter_t,
        out: *mut scf_instance_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_snapshot_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_snapshot_t {
        unimplemented!()
    }
    pub unsafe fn scf_snapshot_destroy(snapshot: *mut scf_snapshot_t) {
        unimplemented!()
    }

    pub unsafe fn scf_snapshot_get_name(
        snapshot: *mut scf_snapshot_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t {
        unimplemented!()
    }
    pub unsafe fn scf_snapshot_get_parent(
        snapshot: *const scf_snapshot_t,
        inst: *mut scf_instance_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_iter_instance_snapshots(
        iter: *mut scf_iter_t,
        instance: *const scf_instance_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_iter_next_snapshot(
        iter: *mut scf_iter_t,
        out: *mut scf_snapshot_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_iter_instance_pgs(
        iter: *mut scf_iter_t,
        instance: *const scf_instance_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_iter_instance_pgs_composed(
        iter: *mut scf_iter_t,
        instance: *const scf_instance_t,
        snapshot: *const scf_snapshot_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_iter_service_pgs(
        iter: *mut scf_iter_t,
        service: *const scf_service_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_iter_next_pg(
        iter: *mut scf_iter_t,
        out: *mut scf_propertygroup_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_pg_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_propertygroup_t {
        unimplemented!()
    }
    pub unsafe fn scf_pg_destroy(pg: *mut scf_propertygroup_t) {
        unimplemented!()
    }

    pub unsafe fn scf_pg_get_name(
        pg: *mut scf_propertygroup_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t {
        unimplemented!()
    }
    pub unsafe fn scf_pg_get_type(
        pg: *mut scf_propertygroup_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t {
        unimplemented!()
    }
    pub unsafe fn scf_pg_get_flags(
        pg: *mut scf_propertygroup_t,
        out: *mut u32,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_pg_update(pg: *mut scf_propertygroup_t) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_pg_get_property(
        pg: *mut scf_propertygroup_t,
        name: *const c_char,
        out: *mut scf_property_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_iter_pg_properties(
        iter: *mut scf_iter_t,
        pg: *const scf_propertygroup_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_iter_next_property(
        iter: *mut scf_iter_t,
        out: *mut scf_property_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_property_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_property_t {
        unimplemented!()
    }
    pub unsafe fn scf_property_destroy(prop: *mut scf_property_t) {
        unimplemented!()
    }

    pub unsafe fn scf_property_get_name(
        prop: *mut scf_property_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t {
        unimplemented!()
    }
    pub unsafe fn scf_property_type(
        prop: *mut scf_property_t,
        typ: *mut scf_type_t,
    ) -> ssize_t {
        unimplemented!()
    }

    pub unsafe fn scf_iter_property_values(
        iter: *mut scf_iter_t,
        prop: *const scf_property_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_iter_next_value(
        iter: *mut scf_iter_t,
        out: *mut scf_value_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_value_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_value_t {
        unimplemented!()
    }
    pub unsafe fn scf_value_destroy(val: *mut scf_value_t) {
        unimplemented!()
    }
    pub unsafe fn scf_value_reset(val: *mut scf_value_t) {
        unimplemented!()
    }

    pub unsafe fn scf_value_type(val: *mut scf_value_t) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_value_base_type(val: *mut scf_value_t) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_value_get_as_string(
        val: *mut scf_value_t,
        buf: *mut c_char,
        size: size_t,
    ) -> ssize_t {
        unimplemented!()
    }
    pub unsafe fn scf_value_set_from_string(
        val: *mut scf_value_t,
        valtype: scf_type_t,
        valstr: *const c_char,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_type_base_type(
        typ: scf_type_t,
        out: *mut scf_type_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_transaction_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_transaction_t {
        unimplemented!()
    }
    pub unsafe fn scf_transaction_destroy(tran: *mut scf_transaction_t) {
        unimplemented!()
    }
    pub unsafe fn scf_transaction_reset(tran: *mut scf_transaction_t) {
        unimplemented!()
    }
    pub unsafe fn scf_transaction_reset_all(tran: *mut scf_transaction_t) {
        unimplemented!()
    }

    pub unsafe fn scf_transaction_start(
        tran: *mut scf_transaction_t,
        pg: *mut scf_propertygroup_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_transaction_commit(
        tran: *mut scf_transaction_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_transaction_property_delete(
        tran: *mut scf_transaction_t,
        entry: *mut scf_transaction_entry_t,
        name: *const c_char,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_transaction_property_new(
        tran: *mut scf_transaction_t,
        entry: *mut scf_transaction_entry_t,
        name: *const c_char,
        proptype: scf_type_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_transaction_property_change(
        tran: *mut scf_transaction_t,
        entry: *mut scf_transaction_entry_t,
        name: *const c_char,
        proptype: scf_type_t,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn scf_transaction_property_change_type(
        tran: *mut scf_transaction_t,
        entry: *mut scf_transaction_entry_t,
        name: *const c_char,
        proptype: scf_type_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn scf_entry_create(
        handle: *mut scf_handle_t,
    ) -> *mut scf_transaction_entry_t {
        unimplemented!()
    }
    pub unsafe fn scf_entry_destroy(tran: *mut scf_transaction_entry_t) {
        unimplemented!()
    }
    pub unsafe fn scf_entry_destroy_children(
        tran: *mut scf_transaction_entry_t,
    ) {
        unimplemented!()
    }
    pub unsafe fn scf_entry_reset(tran: *mut scf_transaction_entry_t) {
        unimplemented!()
    }

    pub unsafe fn scf_entry_add_value(
        tran: *mut scf_transaction_entry_t,
        value: *mut scf_value_t,
    ) -> c_int {
        unimplemented!()
    }

    pub unsafe fn smf_disable_instance(
        instance: *const c_char,
        flags: c_int,
    ) -> c_int {
        unimplemented!()
    }
    pub unsafe fn smf_enable_instance(
        instance: *const c_char,
        flags: c_int,
    ) -> c_int {
        unimplemented!()
    }
}

#[cfg(not(target_os = "illumos"))]
pub use dummy::*;

#[cfg(all(target_os = "illumos", test))]
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
