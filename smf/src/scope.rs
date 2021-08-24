use std::ptr::NonNull;

use super::scf_sys::*;
use super::{buf_for, str_from, Iter, Result, Scf, ScfError, Services};

#[derive(Debug)]
pub struct Scope<'a> {
    pub(crate) scf: &'a Scf,
    pub(crate) scope: NonNull<scf_scope_t>,
}

impl<'a> Scope<'a> {
    pub(crate) fn new(scf: &'a Scf) -> Result<Scope> {
        if let Some(scope) =
            NonNull::new(unsafe { scf_scope_create(scf.handle.as_ptr()) })
        {
            Ok(Scope { scf, scope })
        } else {
            Err(ScfError::last())
        }
    }

    pub fn name(&self) -> Result<String> {
        let mut buf = buf_for(SCF_LIMIT_MAX_NAME_LENGTH)?;

        let ret = unsafe {
            scf_scope_get_name(
                self.scope.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        };

        str_from(&mut buf, ret)
    }

    pub fn services(&self) -> Result<Services> {
        Services::new(self)
    }

    /*
     * XXX fn service(&self, name: &str) -> Result<Service> {
     * scf_scope_get_service(3SCF)
     */

    /*
     * XXX fn add(&self, name: &str) -> Result<Service> {
     * scf_scope_add_service(3SCF)
     */
}

impl Drop for Scope<'_> {
    fn drop(&mut self) {
        unsafe { scf_scope_destroy(self.scope.as_ptr()) };
    }
}

pub struct Scopes<'a> {
    scf: &'a Scf,
    iter: Iter<'a>,
}

impl<'a> Scopes<'a> {
    pub(crate) fn new(scf: &'a Scf) -> Result<Scopes> {
        let iter = Iter::new(scf)?;

        if unsafe {
            scf_iter_handle_scopes(iter.iter.as_ptr(), scf.handle.as_ptr())
        } != 0
        {
            Err(ScfError::last())
        } else {
            Ok(Scopes { scf, iter })
        }
    }

    fn get(&self) -> Result<Option<Scope<'a>>> {
        let scope = Scope::new(self.scf)?;

        let res = unsafe {
            scf_iter_next_scope(self.iter.iter.as_ptr(), scope.scope.as_ptr())
        };

        match res {
            0 => Ok(None),
            1 => Ok(Some(scope)),
            _ => Err(ScfError::last()),
        }
    }
}

impl<'a> Iterator for Scopes<'a> {
    type Item = Result<Scope<'a>>;

    fn next(&mut self) -> Option<Result<Scope<'a>>> {
        self.get().transpose()
    }
}
