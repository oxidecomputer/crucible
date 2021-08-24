use std::ptr::NonNull;

use super::libscf::*;
use super::{
    buf_for, str_from, Instance, Iter, Properties, Result, Scf, ScfError,
    Service, Snapshot,
};

#[derive(Debug)]
pub struct PropertyGroup<'a> {
    pub(crate) scf: &'a Scf,
    pub(crate) propertygroup: NonNull<scf_propertygroup_t>,
}

impl<'a> PropertyGroup<'a> {
    pub(crate) fn new(scf: &'a Scf) -> Result<PropertyGroup> {
        if let Some(propertygroup) =
            NonNull::new(unsafe { scf_pg_create(scf.handle.as_ptr()) })
        {
            Ok(PropertyGroup { scf, propertygroup })
        } else {
            Err(ScfError::last())
        }
    }

    pub fn name(&self) -> Result<String> {
        let mut buf = buf_for(SCF_LIMIT_MAX_NAME_LENGTH)?;

        let ret = unsafe {
            scf_pg_get_name(
                self.propertygroup.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        };

        str_from(&mut buf, ret)
    }

    pub fn type_(&self) -> Result<String> {
        let mut buf = buf_for(SCF_LIMIT_MAX_PG_TYPE_LENGTH)?;

        let ret = unsafe {
            scf_pg_get_type(
                self.propertygroup.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        };

        str_from(&mut buf, ret)
    }

    pub fn properties(&self) -> Result<Properties> {
        Properties::new(self)
    }

    pub fn is_persistent(&self) -> Result<bool> {
        let mut flags = 0;

        if unsafe { scf_pg_get_flags(self.propertygroup.as_ptr(), &mut flags) }
            == 0
        {
            Ok((flags & SCF_PG_FLAG_NONPERSISTENT) == 0)
        } else {
            Err(ScfError::last())
        }
    }

    /*
     * XXX fn type_(&self) -> Result<String> {
     * scf_pg_get_type(3SCF)
     */

    /*
     * XXX fn flags(&self) -> Result<u32> {
     * scf_pg_get_flags(3SCF)
     */

    /*
     * XXX fn delete(&self) -> Result<()> {
     * scf_pg_delete(3SCF)
     */

    /*
     * XXX fn update(&self) -> Result<()> {
     * scf_pg_update(3SCF)
     */
}

impl Drop for PropertyGroup<'_> {
    fn drop(&mut self) {
        unsafe { scf_pg_destroy(self.propertygroup.as_ptr()) };
    }
}

pub struct PropertyGroups<'a> {
    pub(crate) scf: &'a Scf,
    pub(crate) iter: Iter<'a>,
}

impl<'a> PropertyGroups<'a> {
    pub(crate) fn new_instance(
        instance: &'a Instance,
    ) -> Result<PropertyGroups<'a>> {
        let scf = instance.scf;
        let iter = Iter::new(scf)?;

        if unsafe {
            scf_iter_instance_pgs(
                iter.iter.as_ptr(),
                instance.instance.as_ptr(),
            )
        } != 0
        {
            Err(ScfError::last())
        } else {
            Ok(PropertyGroups { scf, iter })
        }
    }

    pub(crate) fn new_composed(
        instance: &'a Instance,
        snapshot: &'a Snapshot,
    ) -> Result<PropertyGroups<'a>> {
        let scf = instance.scf;
        let iter = Iter::new(scf)?;

        if unsafe {
            scf_iter_instance_pgs_composed(
                iter.iter.as_ptr(),
                instance.instance.as_ptr(),
                snapshot.snapshot.as_ptr(),
            )
        } != 0
        {
            Err(ScfError::last())
        } else {
            Ok(PropertyGroups { scf, iter })
        }
    }

    pub(crate) fn new_service(
        service: &'a Service,
    ) -> Result<PropertyGroups<'a>> {
        let scf = service.scf;
        let iter = Iter::new(scf)?;

        if unsafe {
            scf_iter_service_pgs(iter.iter.as_ptr(), service.service.as_ptr())
        } != 0
        {
            Err(ScfError::last())
        } else {
            Ok(PropertyGroups { scf, iter })
        }
    }

    fn get(&self) -> Result<Option<PropertyGroup<'a>>> {
        let propertygroup = PropertyGroup::new(self.scf)?;

        let res = unsafe {
            scf_iter_next_pg(
                self.iter.iter.as_ptr(),
                propertygroup.propertygroup.as_ptr(),
            )
        };

        match res {
            0 => Ok(None),
            1 => Ok(Some(propertygroup)),
            _ => Err(ScfError::last()),
        }
    }
}

impl<'a> Iterator for PropertyGroups<'a> {
    type Item = Result<PropertyGroup<'a>>;

    fn next(&mut self) -> Option<Result<PropertyGroup<'a>>> {
        self.get().transpose()
    }
}
