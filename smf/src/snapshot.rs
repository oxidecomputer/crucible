// Copyright 2021 Oxide Computer Company

use std::ffi::CString;
use std::ptr::NonNull;

use super::scf_sys::*;
use super::{
    buf_for, str_from, Instance, Iter, PropertyGroup, PropertyGroups, Result,
    ScfError,
};

#[derive(Debug)]
pub struct Snapshot<'a> {
    pub(crate) instance: &'a Instance<'a>,
    pub(crate) snapshot: NonNull<scf_snapshot_t>,
}

impl<'a> Snapshot<'a> {
    pub(crate) fn new(instance: &'a Instance) -> Result<Snapshot<'a>> {
        if let Some(snapshot) = NonNull::new(unsafe {
            scf_snapshot_create(instance.scf.handle.as_ptr())
        }) {
            Ok(Snapshot { instance, snapshot })
        } else {
            Err(ScfError::last())
        }
    }

    pub fn name(&self) -> Result<String> {
        let mut buf = buf_for(SCF_LIMIT_MAX_NAME_LENGTH)?;

        let ret = unsafe {
            scf_snapshot_get_name(
                self.snapshot.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        };

        str_from(&mut buf, ret)
    }

    pub fn pgs(&self) -> Result<PropertyGroups<'_>> {
        PropertyGroups::new_composed(self.instance, self)
    }

    pub fn get_pg(&self, name: &str) -> Result<Option<PropertyGroup<'_>>> {
        let name = CString::new(name).unwrap();
        let pg = PropertyGroup::new(self.instance.scf)?;

        let ret = unsafe {
            scf_instance_get_pg_composed(
                self.instance.instance.as_ptr(),
                self.snapshot.as_ptr(),
                name.as_ptr(),
                pg.propertygroup.as_ptr(),
            )
        };

        if ret == 0 {
            Ok(Some(pg))
        } else {
            match ScfError::last() {
                ScfError::NotFound => Ok(None),
                other => Err(other),
            }
        }
    }

    /*
     * XXX fn delete(&self) -> Result<()> {
     * scf_snapshot_delete(3SCF)
     */
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        unsafe { scf_snapshot_destroy(self.snapshot.as_ptr()) };
    }
}

pub struct Snapshots<'a> {
    pub(crate) instance: &'a Instance<'a>,
    pub(crate) iter: Iter<'a>,
}

impl<'a> Snapshots<'a> {
    pub(crate) fn new(instance: &'a Instance) -> Result<Snapshots<'a>> {
        let iter = Iter::new(instance.scf)?;

        if unsafe {
            scf_iter_instance_snapshots(
                iter.iter.as_ptr(),
                instance.instance.as_ptr(),
            )
        } != 0
        {
            Err(ScfError::last())
        } else {
            Ok(Snapshots { instance, iter })
        }
    }

    fn get(&self) -> Result<Option<Snapshot<'a>>> {
        let snapshot = Snapshot::new(self.instance)?;

        let res = unsafe {
            scf_iter_next_snapshot(
                self.iter.iter.as_ptr(),
                snapshot.snapshot.as_ptr(),
            )
        };

        match res {
            0 => Ok(None),
            1 => Ok(Some(snapshot)),
            _ => Err(ScfError::last()),
        }
    }
}

impl<'a> Iterator for Snapshots<'a> {
    type Item = Result<Snapshot<'a>>;

    fn next(&mut self) -> Option<Result<Snapshot<'a>>> {
        self.get().transpose()
    }
}
