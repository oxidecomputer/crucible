use std::ptr::NonNull;

use num_traits::cast::FromPrimitive;

use super::libscf::*;
use super::{buf_for, str_from, Service, Instance, Snapshot, Property, Iter, Result, Scf, ScfError};

#[derive(Debug)]
pub struct Value<'a> {
    pub(crate) scf: &'a Scf,
    pub(crate) value: NonNull<scf_value_t>,
}

impl<'a> Value<'a> {
    pub(crate) fn new(scf: &'a Scf) -> Result<Value> {
        if let Some(value) =
            NonNull::new(unsafe { scf_value_create(scf.handle.as_ptr()) })
        {
            Ok(Value { scf, value })
        } else {
            Err(ScfError::last())
        }
    }

    pub fn as_string(&self) -> Result<String> {
        let mut buf = buf_for(SCF_LIMIT_MAX_VALUE_LENGTH)?;

        let ret = unsafe {
            scf_value_get_as_string(
                self.value.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        };

        str_from(&mut buf, ret)
    }

    pub fn type_(&self) -> Result<scf_type_t> {
        let mut typ: scf_type_t = scf_type_t::SCF_TYPE_INVALID;

        let ret = unsafe { scf_value_type(self.value.as_ptr()) };
        match scf_type_t::from_i32(ret) {
            Some(scf_type_t::SCF_TYPE_INVALID) => Err(ScfError::last()),
            Some(typ) => Ok(typ),
            None => Err(ScfError::Internal),
        }
    }

    pub fn base_type(&self) -> Result<scf_type_t> {
        let mut typ: scf_type_t = scf_type_t::SCF_TYPE_INVALID;

        let ret = unsafe { scf_value_base_type(self.value.as_ptr()) };
        match scf_type_t::from_i32(ret) {
            Some(scf_type_t::SCF_TYPE_INVALID) => Err(ScfError::last()),
            Some(typ) => Ok(typ),
            None => Err(ScfError::Internal),
        }
    }

    /*
     * XXX fn value(&self) -> Result<Value> {
     * scf_value_get_value(3SCF)
     */

    /*
     * XXX fn values(&self) -> Result<Values> {
     * scf_iter_value_values(3SCF)
     */
}

impl Drop for Value<'_> {
    fn drop(&mut self) {
        unsafe { scf_value_destroy(self.value.as_ptr()) };
    }
}

pub struct Values<'a> {
    pub(crate) scf: &'a Scf,
    pub(crate) iter: Iter<'a>,
}

impl<'a> Values<'a> {
    pub(crate) fn new(
        prop: &'a Property,
    ) -> Result<Values<'a>> {
        let scf = prop.scf;
        let iter = Iter::new(scf)?;

        if unsafe {
            scf_iter_property_values(
                iter.iter.as_ptr(),
                prop.property.as_ptr(),
            )
        } != 0
        {
            Err(ScfError::last())
        } else {
            Ok(Values { scf, iter })
        }
    }

    fn get(&self) -> Result<Option<Value<'a>>> {
        let value = Value::new(self.scf)?;

        let res = unsafe {
            scf_iter_next_value(
                self.iter.iter.as_ptr(),
                value.value.as_ptr(),
            )
        };

        match res {
            0 => Ok(None),
            1 => Ok(Some(value)),
            _ => Err(ScfError::last()),
        }
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = Result<Value<'a>>;

    fn next(&mut self) -> Option<Result<Value<'a>>> {
        self.get().transpose()
    }
}
