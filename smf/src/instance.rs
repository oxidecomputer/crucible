use std::ptr::NonNull;

use super::libscf::*;
use super::{
    buf_for, str_from, Iter, PropertyGroups, Result, Scf, ScfError, Service,
    Snapshots,
};

#[derive(Debug)]
pub struct Instance<'a> {
    pub(crate) scf: &'a Scf,
    pub(crate) instance: NonNull<scf_instance_t>,
}

impl<'a> Instance<'a> {
    pub(crate) fn new(scf: &'a Scf) -> Result<Instance> {
        if let Some(instance) =
            NonNull::new(unsafe { scf_instance_create(scf.handle.as_ptr()) })
        {
            Ok(Instance { scf, instance })
        } else {
            Err(ScfError::last())
        }
    }

    pub fn name(&self) -> Result<String> {
        let mut buf = buf_for(SCF_LIMIT_MAX_NAME_LENGTH)?;

        let ret = unsafe {
            scf_instance_get_name(
                self.instance.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        };

        str_from(&mut buf, ret)
    }

    pub fn snapshots(&self) -> Result<Snapshots> {
        Snapshots::new(self)
    }

    pub fn pgs(&self) -> Result<PropertyGroups> {
        PropertyGroups::new_instance(self)
    }

    /*
     * XXX fn delete(&self) -> Result<()> {
     * scf_instance_delete(3SCF)
     */
}

impl Drop for Instance<'_> {
    fn drop(&mut self) {
        unsafe { scf_instance_destroy(self.instance.as_ptr()) };
    }
}

pub struct Instances<'a> {
    pub(crate) service: &'a Service<'a>,
    pub(crate) iter: Iter<'a>,
}

impl<'a> Instances<'a> {
    pub(crate) fn new(service: &'a Service) -> Result<Instances<'a>> {
        let iter = Iter::new(service.scf)?;

        if unsafe {
            scf_iter_service_instances(
                iter.iter.as_ptr(),
                service.service.as_ptr(),
            )
        } != 0
        {
            Err(ScfError::last())
        } else {
            Ok(Instances { service, iter })
        }
    }

    fn get(&self) -> Result<Option<Instance<'a>>> {
        let instance = Instance::new(self.service.scf)?;

        let res = unsafe {
            scf_iter_next_instance(
                self.iter.iter.as_ptr(),
                instance.instance.as_ptr(),
            )
        };

        match res {
            0 => Ok(None),
            1 => Ok(Some(instance)),
            _ => Err(ScfError::last()),
        }
    }
}

impl<'a> Iterator for Instances<'a> {
    type Item = Result<Instance<'a>>;

    fn next(&mut self) -> Option<Result<Instance<'a>>> {
        self.get().transpose()
    }
}
