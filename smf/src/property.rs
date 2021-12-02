use std::ptr::NonNull;

use super::scf_sys::*;
use super::{
    buf_for, str_from, Iter, PropertyGroup, Result, Scf, ScfError, Value,
    Values,
};

#[derive(Debug)]
pub struct Property<'a> {
    pub(crate) scf: &'a Scf,
    pub(crate) property: NonNull<scf_property_t>,
}

impl<'a> Property<'a> {
    pub(crate) fn new(scf: &'a Scf) -> Result<Property> {
        if let Some(property) =
            NonNull::new(unsafe { scf_property_create(scf.handle.as_ptr()) })
        {
            Ok(Property { scf, property })
        } else {
            Err(ScfError::last())
        }
    }

    pub fn name(&self) -> Result<String> {
        let mut buf = buf_for(SCF_LIMIT_MAX_NAME_LENGTH)?;

        let ret = unsafe {
            scf_property_get_name(
                self.property.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        };

        str_from(&mut buf, ret)
    }

    pub fn type_(&self) -> Result<scf_type_t> {
        let mut typ: scf_type_t = scf_type_t::SCF_TYPE_INVALID;

        if unsafe { scf_property_type(self.property.as_ptr(), &mut typ) } == 0 {
            Ok(typ)
        } else {
            Err(ScfError::last())
        }
    }

    /*
     * XXX fn value(&self) -> Result<Value> {
     * scf_property_get_value(3SCF)
     */

    pub fn values(&self) -> Result<Values> {
        Values::new(self)
    }

    pub fn value(&self) -> Result<Option<Value>> {
        let mut values = Values::new(self)?.collect::<Result<Vec<_>>>()?;
        match values.len() {
            0 => Ok(None),
            1 => Ok(values.pop()),
            _ => Err(ScfError::Internal),
        }
    }
}

impl Drop for Property<'_> {
    fn drop(&mut self) {
        unsafe { scf_property_destroy(self.property.as_ptr()) };
    }
}

pub struct Properties<'a> {
    pub(crate) scf: &'a Scf,
    pub(crate) iter: Iter<'a>,
}

impl<'a> Properties<'a> {
    pub(crate) fn new(pg: &'a PropertyGroup) -> Result<Properties<'a>> {
        let scf = pg.scf;
        let iter = Iter::new(scf)?;

        if unsafe {
            scf_iter_pg_properties(
                iter.iter.as_ptr(),
                pg.propertygroup.as_ptr(),
            )
        } != 0
        {
            Err(ScfError::last())
        } else {
            Ok(Properties { scf, iter })
        }
    }

    fn get(&self) -> Result<Option<Property<'a>>> {
        let property = Property::new(self.scf)?;

        let res = unsafe {
            scf_iter_next_property(
                self.iter.iter.as_ptr(),
                property.property.as_ptr(),
            )
        };

        match res {
            0 => Ok(None),
            1 => Ok(Some(property)),
            _ => Err(ScfError::last()),
        }
    }
}

impl<'a> Iterator for Properties<'a> {
    type Item = Result<Property<'a>>;

    fn next(&mut self) -> Option<Result<Property<'a>>> {
        self.get().transpose()
    }
}
