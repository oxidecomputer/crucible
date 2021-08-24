use std::ptr::NonNull;

use super::libscf::*;
use super::{
    buf_for, str_from, Instance, Iter, PropertyGroups, Result, ScfError,
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

    pub fn pgs(&self) -> Result<PropertyGroups> {
        PropertyGroups::new_composed(self.instance, self)
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
