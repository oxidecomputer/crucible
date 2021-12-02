// Copyright 2021 Oxide Computer Company

use std::ffi::CString;
use std::ptr::NonNull;

use super::scf_sys::*;
use super::{PropertyGroup, Result, ScfError, Value};

#[derive(Debug)]
pub struct Transaction<'a> {
    pub(crate) pg: &'a PropertyGroup<'a>,
    pub(crate) transaction: NonNull<scf_transaction_t>,
}

pub enum CommitResult {
    Success,
    OutOfDate,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(pg: &'a PropertyGroup<'a>) -> Result<Transaction> {
        let scf = pg.scf;

        if let Some(transaction) =
            NonNull::new(unsafe { scf_transaction_create(scf.handle.as_ptr()) })
        {
            Ok(Transaction { pg, transaction })
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

    pub fn property_delete(&self, name: &str) -> Result<TransactionEntry> {
        let name = CString::new(name).unwrap();
        let ent = TransactionEntry::new(self)?;

        let ret = unsafe {
            scf_transaction_property_delete(
                self.transaction.as_ptr(),
                ent.entry.as_ptr(),
                name.as_ptr(),
            )
        };

        if ret == 0 {
            Ok(ent)
        } else {
            Err(ScfError::last())
        }
    }

    pub fn property_new(
        &self,
        name: &str,
        typ: scf_type_t,
    ) -> Result<TransactionEntry> {
        let name = CString::new(name).unwrap();
        let ent = TransactionEntry::new(self)?;

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

    pub fn property_change(
        &self,
        name: &str,
        typ: scf_type_t,
    ) -> Result<TransactionEntry> {
        let name = CString::new(name).unwrap();
        let ent = TransactionEntry::new(self)?;

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
pub struct TransactionEntry<'a> {
    pub(crate) transaction: &'a Transaction<'a>,
    pub(crate) entry: NonNull<scf_transaction_entry_t>,
    values: Vec<Value<'a>>,
}

impl<'a> TransactionEntry<'a> {
    pub(crate) fn new(tx: &'a Transaction<'a>) -> Result<TransactionEntry> {
        let scf = tx.pg.scf;

        if let Some(entry) =
            NonNull::new(unsafe { scf_entry_create(scf.handle.as_ptr()) })
        {
            Ok(TransactionEntry {
                transaction: tx,
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
        let v = Value::new(self.transaction.pg.scf)?;
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
