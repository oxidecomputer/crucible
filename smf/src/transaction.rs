// Copyright 2021 Oxide Computer Company

use std::ffi::CString;
use std::ptr::NonNull;
use std::sync::Mutex;

use super::scf_sys::*;
use super::{PropertyGroup, Result, Scf, ScfError, Value};

#[derive(Debug)]
pub struct Transaction<'a> {
    pub(crate) pg: &'a PropertyGroup<'a>,
    pub(crate) transaction: NonNull<scf_transaction_t>,

    /*
     * Keep TransactionEntry objects around - if they are dropped, then the
     * transaction is invalidated.
     */
    transaction_entries: Mutex<Vec<TransactionEntry<'a>>>,
}

pub enum CommitResult {
    Success,
    OutOfDate,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(pg: &'a PropertyGroup<'a>) -> Result<Transaction<'a>> {
        let scf = pg.scf;

        if let Some(transaction) =
            NonNull::new(unsafe { scf_transaction_create(scf.handle.as_ptr()) })
        {
            Ok(Transaction {
                pg,
                transaction,
                transaction_entries: Mutex::new(vec![]),
            })
        } else {
            Err(ScfError::last())
        }
    }

    pub fn start(&self) -> Result<()> {
        if unsafe {
            scf_transaction_start(
                self.transaction.as_ptr(),
                self.pg.propertygroup.as_ptr(),
            )
        } == 0
        {
            Ok(())
        } else {
            Err(ScfError::last())
        }
    }

    pub fn commit(&self) -> Result<CommitResult> {
        match unsafe { scf_transaction_commit(self.transaction.as_ptr()) } {
            0 => Ok(CommitResult::OutOfDate),
            1 => Ok(CommitResult::Success),
            _ => Err(ScfError::last()),
        }
    }

    pub fn reset(&self) {
        unsafe { scf_transaction_reset(self.transaction.as_ptr()) };
    }

    pub fn reset_all(&self) {
        unsafe { scf_transaction_reset_all(self.transaction.as_ptr()) };
    }

    pub fn property_delete(&self, name: &str) -> Result<()> {
        let name = CString::new(name).unwrap();
        let ent = TransactionEntry::new(self.pg.scf)?;

        let ret = unsafe {
            scf_transaction_property_delete(
                self.transaction.as_ptr(),
                ent.entry.as_ptr(),
                name.as_ptr(),
            )
        };

        self.transaction_entries.lock().unwrap().push(ent);

        if ret == 0 {
            Ok(())
        } else {
            Err(ScfError::last())
        }
    }

    fn property_new(
        &self,
        name: &str,
        typ: scf_type_t,
    ) -> Result<TransactionEntry<'a>> {
        let name = CString::new(name).unwrap();
        let ent = TransactionEntry::new(self.pg.scf)?;

        let ret = unsafe {
            scf_transaction_property_new(
                self.transaction.as_ptr(),
                ent.entry.as_ptr(),
                name.as_ptr(),
                typ,
            )
        };

        if ret == 0 {
            Ok(ent)
        } else {
            Err(ScfError::last())
        }
    }

    fn property_change(
        &self,
        name: &str,
        typ: scf_type_t,
    ) -> Result<TransactionEntry<'a>> {
        let name = CString::new(name).unwrap();
        let ent = TransactionEntry::new(self.pg.scf)?;

        let ret = unsafe {
            scf_transaction_property_change(
                self.transaction.as_ptr(),
                ent.entry.as_ptr(),
                name.as_ptr(),
                typ,
            )
        };

        if ret == 0 {
            Ok(ent)
        } else {
            Err(ScfError::last())
        }
    }

    /**
     * Either add or change an existing property.
     */
    pub fn property_ensure(
        &self,
        name: &str,
        typ: scf_type_t,
        val: &str,
    ) -> Result<()> {
        let mut e = if self.pg.get_property(name)?.is_some() {
            self.property_change(name, typ)?
        } else {
            self.property_new(name, typ)?
        };
        e.add_from_string(typ, val)?;

        self.transaction_entries.lock().unwrap().push(e);

        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        /*
         * Call scf_transaction_reset_all(3SCF) to reset any entries that are
         * associated with this transaction before we destroy it.
         */
        self.reset_all();
        unsafe { scf_transaction_destroy(self.transaction.as_ptr()) };
    }
}

#[derive(Debug)]
struct TransactionEntry<'a> {
    pub(crate) scf: &'a Scf,
    pub(crate) entry: NonNull<scf_transaction_entry_t>,
    values: Vec<Value<'a>>,
}

impl<'a> TransactionEntry<'a> {
    pub(crate) fn new(scf: &'a Scf) -> Result<TransactionEntry<'a>> {
        if let Some(entry) =
            NonNull::new(unsafe { scf_entry_create(scf.handle.as_ptr()) })
        {
            Ok(TransactionEntry {
                scf,
                entry,
                values: Vec::new(),
            })
        } else {
            Err(ScfError::last())
        }
    }

    pub fn add_from_string(
        &mut self,
        typ: scf_type_t,
        val: &str,
    ) -> Result<()> {
        let v = Value::new(self.scf)?;
        v.set_from_string(typ, val)?;
        if unsafe { scf_entry_add_value(self.entry.as_ptr(), v.value.as_ptr()) }
            == 0
        {
            /*
             * Keep the Value around so that we can free it later when we
             * drop this entry.
             */
            self.values.push(v);
            Ok(())
        } else {
            Err(ScfError::last())
        }
    }
}

impl Drop for TransactionEntry<'_> {
    fn drop(&mut self) {
        /*
         * First, reset any Values that might have been associated.  They'll
         * get freed separately in their own drop actions.
         */
        unsafe { scf_entry_reset(self.entry.as_ptr()) };
        unsafe { scf_entry_destroy(self.entry.as_ptr()) };
    }
}
