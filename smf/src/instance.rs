// Copyright 2021 Oxide Computer Company

use std::ffi::CString;
use std::ptr::NonNull;

use super::scf_sys::*;
use super::{
    buf_for, str_from, Iter, PropertyGroup, PropertyGroups, Result, Scf,
    ScfError, Service, Snapshot, Snapshots,
};

#[derive(Debug)]
pub struct Instance<'a> {
    pub(crate) scf: &'a Scf,
    pub(crate) instance: NonNull<scf_instance_t>,
}

#[derive(Debug)]
pub enum State {
    Uninitialized,
    Maintenance,
    Offline,
    Disabled,
    Online,
    Degraded,
    LegacyRun,
    Unknown(String),
}

impl State {
    fn parse(v: &str) -> Option<State> {
        use State::*;

        match v {
            "none" => None,
            "uninitialized" => Some(Uninitialized),
            "maintenance" => Some(Maintenance),
            "offline" => Some(Offline),
            "disabled" => Some(Disabled),
            "online" => Some(Online),
            "degraded" => Some(Degraded),
            "legacy_run" => Some(LegacyRun),
            other => Some(Unknown(other.to_string())),
        }
    }
}

impl<'a> Instance<'a> {
    pub(crate) fn new(scf: &'a Scf) -> Result<Instance<'a>> {
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

    pub fn fmri(&self) -> Result<String> {
        let mut buf = buf_for(SCF_LIMIT_MAX_FMRI_LENGTH)?;

        let ret = unsafe {
            scf_instance_to_fmri(
                self.instance.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        };

        str_from(&mut buf, ret)
    }

    pub fn enable(&self, temporary: bool) -> Result<()> {
        let fmri = CString::new(self.fmri()?).unwrap();
        let flags = if temporary { SMF_TEMPORARY } else { 0 };

        if unsafe { smf_enable_instance(fmri.as_ptr(), flags) } == 0 {
            Ok(())
        } else {
            Err(ScfError::last())
        }
    }

    pub fn disable(&self, temporary: bool) -> Result<()> {
        let fmri = CString::new(self.fmri()?).unwrap();
        let flags = if temporary { SMF_TEMPORARY } else { 0 };

        if unsafe { smf_disable_instance(fmri.as_ptr(), flags) } == 0 {
            Ok(())
        } else {
            Err(ScfError::last())
        }
    }

    /**
     * Refresh this instance with any changes to its configuration.
     */
    pub fn refresh(&self) -> Result<()> {
        let fmri = CString::new(self.fmri()?).unwrap();
        if unsafe { smf_refresh_instance(fmri.as_ptr()) } == 0 {
            Ok(())
        } else {
            Err(ScfError::last())
        }
    }

    /**
     * Get the current state and the next state (if any) of the instance
     * from the restarter.
     */
    pub fn states(&self) -> Result<(Option<State>, Option<State>)> {
        let restarter = self.get_pg("restarter")?.ok_or(ScfError::NotFound)?;

        fn get_val(pg: &PropertyGroup, n: &str) -> Result<Option<String>> {
            let prop = pg.get_property(n)?;
            if let Some(prop) = prop {
                let val = prop.value()?;
                if let Some(val) = val {
                    Ok(Some(val.as_string()?))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }

        let state =
            get_val(&restarter, "state")?.and_then(|v| State::parse(&v));
        let next_state =
            get_val(&restarter, "next_state")?.and_then(|v| State::parse(&v));

        Ok((state, next_state))
    }

    pub fn snapshots(&self) -> Result<Snapshots<'_>> {
        Snapshots::new(self)
    }

    /**
     * Helper function to get the running snapshot for this instance, if one
     * exists.
     *
     * # Errors
     *
     * Returns [`ScfError::NoRunningSnapshot`] if this instance does not have a
     * `running` snapshot. May return other errors if querying for the running
     * snapshot fails.
     */
    pub fn get_running_snapshot(&self) -> Result<Snapshot<'_>> {
        let maybe_snapshot = self.get_snapshot("running")?;
        maybe_snapshot.ok_or(ScfError::NoRunningSnapshot)
    }

    pub fn get_snapshot(&self, name: &str) -> Result<Option<Snapshot<'_>>> {
        let name = CString::new(name).unwrap();
        let snap = Snapshot::new(self)?;

        let ret = unsafe {
            scf_instance_get_snapshot(
                self.instance.as_ptr(),
                name.as_ptr(),
                snap.snapshot.as_ptr(),
            )
        };

        if ret == 0 {
            Ok(Some(snap))
        } else {
            match ScfError::last() {
                ScfError::NotFound => Ok(None),
                other => Err(other),
            }
        }
    }

    pub fn pgs(&self) -> Result<PropertyGroups<'_>> {
        PropertyGroups::new_instance(self)
    }

    pub fn get_pg(&self, name: &str) -> Result<Option<PropertyGroup<'_>>> {
        let name = CString::new(name).unwrap();
        let pg = PropertyGroup::new(self.scf)?;

        let ret = unsafe {
            scf_instance_get_pg(
                self.instance.as_ptr(),
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

    pub fn add_pg(
        &self,
        name: &str,
        pgtype: &str,
    ) -> Result<PropertyGroup<'_>> {
        let name = CString::new(name).unwrap();
        let pgtype = CString::new(pgtype).unwrap();
        let pg = PropertyGroup::new(self.scf)?;

        let ret = unsafe {
            scf_instance_add_pg(
                self.instance.as_ptr(),
                name.as_ptr(),
                pgtype.as_ptr(),
                0,
                pg.propertygroup.as_ptr(),
            )
        };

        if ret == 0 {
            Ok(pg)
        } else {
            Err(ScfError::last())
        }
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
