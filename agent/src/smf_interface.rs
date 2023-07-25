// Copyright 2023 Oxide Computer Company

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

/// Interfaces for an implementation to interface with SMF objects.

pub trait SmfInterface {
    fn instances(
        &self,
    ) -> Result<Vec<Box<dyn SmfInstance + '_>>, crucible_smf::ScfError>;

    fn get_instance(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfInstance + '_>>, crucible_smf::ScfError>;

    fn add_instance(
        &self,
        name: &str,
    ) -> Result<Box<dyn SmfInstance + '_>, crucible_smf::ScfError>;

    #[cfg(test)]
    fn prune(&self);
}

pub struct RealSmf<'a> {
    svc: &'a crucible_smf::Service<'a>,
}

impl<'a> RealSmf<'a> {
    pub fn new(svc: &'a crucible_smf::Service<'a>) -> Result<RealSmf<'a>> {
        Ok(RealSmf { svc })
    }
}

impl<'a> SmfInterface for RealSmf<'a> {
    fn instances(
        &self,
    ) -> Result<Vec<Box<dyn SmfInstance + '_>>, crucible_smf::ScfError> {
        let smf_insts: Vec<crucible_smf::Instance<'_>> = self
            .svc
            .instances()?
            .collect::<Vec<Result<_, _>>>()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let mut insts: Vec<Box<dyn SmfInstance>> =
            Vec::with_capacity(smf_insts.len());
        for smf_inst in smf_insts {
            insts.push(Box::new(RealSmfInstance::new(smf_inst)));
        }

        Ok(insts)
    }

    fn get_instance(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfInstance + '_>>, crucible_smf::ScfError> {
        let inst = self.svc.get_instance(name)?;
        if let Some(inst) = inst {
            Ok(Some(Box::new(RealSmfInstance::new(inst))))
        } else {
            Ok(None)
        }
    }

    fn add_instance(
        &self,
        name: &str,
    ) -> Result<Box<dyn SmfInstance + '_>, crucible_smf::ScfError> {
        let inst = self.svc.add_instance(name)?;
        Ok(Box::new(RealSmfInstance::new(inst)))
    }

    #[cfg(test)]
    fn prune(&self) {
        todo!()
    }
}

pub trait SmfInstance {
    fn name(&self) -> Result<String, crucible_smf::ScfError>;
    fn fmri(&self) -> Result<String, crucible_smf::ScfError>;
    fn states(
        &self,
    ) -> Result<
        (Option<crucible_smf::State>, Option<crucible_smf::State>),
        crucible_smf::ScfError,
    >;
    fn disable(&self, temporary: bool) -> Result<(), crucible_smf::ScfError>;
    fn enable(&self, temporary: bool) -> Result<(), crucible_smf::ScfError>;
    fn enabled(&self) -> bool;

    fn get_pg(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfPropertyGroup + '_>>, crucible_smf::ScfError>;
    fn add_pg(
        &self,
        name: &str,
        pgtype: &str,
    ) -> Result<Box<dyn SmfPropertyGroup + '_>, crucible_smf::ScfError>;
    fn get_snapshot(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfSnapshot + '_>>, crucible_smf::ScfError>;
}

pub struct RealSmfInstance<'a> {
    inst: crucible_smf::Instance<'a>,
}

impl<'a> RealSmfInstance<'a> {
    pub fn new(inst: crucible_smf::Instance<'a>) -> RealSmfInstance {
        RealSmfInstance { inst }
    }
}

impl<'a> SmfInstance for RealSmfInstance<'a> {
    fn name(&self) -> Result<String, crucible_smf::ScfError> {
        self.inst.name()
    }

    fn fmri(&self) -> Result<String, crucible_smf::ScfError> {
        self.inst.fmri()
    }

    fn states(
        &self,
    ) -> Result<
        (Option<crucible_smf::State>, Option<crucible_smf::State>),
        crucible_smf::ScfError,
    > {
        self.inst.states()
    }

    fn disable(&self, temporary: bool) -> Result<(), crucible_smf::ScfError> {
        self.inst.disable(temporary)
    }

    fn enable(&self, temporary: bool) -> Result<(), crucible_smf::ScfError> {
        self.inst.enable(temporary)
    }

    fn enabled(&self) -> bool {
        unimplemented!();
    }

    fn get_pg(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfPropertyGroup + '_>>, crucible_smf::ScfError>
    {
        if let Some(pg) = self.inst.get_pg(name)? {
            Ok(Some(Box::new(RealSmfPropertyGroup::new(pg))))
        } else {
            Ok(None)
        }
    }

    fn add_pg(
        &self,
        name: &str,
        pgtype: &str,
    ) -> Result<Box<dyn SmfPropertyGroup + '_>, crucible_smf::ScfError> {
        let pg = self.inst.add_pg(name, pgtype)?;
        Ok(Box::new(RealSmfPropertyGroup::new(pg)))
    }

    fn get_snapshot(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfSnapshot + '_>>, crucible_smf::ScfError> {
        let snap = self.inst.get_snapshot(name)?;
        if let Some(snap) = snap {
            Ok(Some(Box::new(RealSmfSnapshot::new(snap))))
        } else {
            Ok(None)
        }
    }
}

pub trait SmfSnapshot {
    fn get_pg(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfPropertyGroup + '_>>, crucible_smf::ScfError>;
}

pub struct RealSmfSnapshot<'a> {
    snap: crucible_smf::Snapshot<'a>,
}

impl<'a> RealSmfSnapshot<'a> {
    pub fn new(snap: crucible_smf::Snapshot<'a>) -> RealSmfSnapshot {
        RealSmfSnapshot { snap }
    }
}

impl<'a> SmfSnapshot for RealSmfSnapshot<'a> {
    fn get_pg(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfPropertyGroup + '_>>, crucible_smf::ScfError>
    {
        if let Some(pg) = self.snap.get_pg(name)? {
            Ok(Some(Box::new(RealSmfPropertyGroup::new(pg))))
        } else {
            Ok(None)
        }
    }
}

pub trait SmfPropertyGroup {
    fn get_property(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfProperty + '_>>, crucible_smf::ScfError>;

    fn transaction(
        &self,
    ) -> Result<Box<dyn SmfTransaction + '_>, crucible_smf::ScfError>;
}

pub struct RealSmfPropertyGroup<'a> {
    pg: crucible_smf::PropertyGroup<'a>,
}

impl<'a> RealSmfPropertyGroup<'a> {
    pub fn new(pg: crucible_smf::PropertyGroup<'a>) -> RealSmfPropertyGroup {
        RealSmfPropertyGroup { pg }
    }
}

impl<'a> SmfPropertyGroup for RealSmfPropertyGroup<'a> {
    fn get_property(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfProperty + '_>>, crucible_smf::ScfError> {
        let p = self.pg.get_property(name)?;
        Ok(Some(Box::new(RealSmfProperty::new(p))))
    }

    fn transaction(
        &self,
    ) -> Result<Box<dyn SmfTransaction + '_>, crucible_smf::ScfError> {
        let t = self.pg.transaction()?;
        Ok(Box::new(RealSmfTransaction::new(t)))
    }
}

pub trait SmfProperty {
    fn value(
        &self,
    ) -> Result<Option<Box<dyn SmfValue + '_>>, crucible_smf::ScfError>;
}

pub struct RealSmfProperty<'a> {
    p: Option<crucible_smf::Property<'a>>,
}

impl<'a> RealSmfProperty<'a> {
    pub fn new(p: Option<crucible_smf::Property<'a>>) -> RealSmfProperty<'a> {
        RealSmfProperty { p }
    }
}

impl<'a> SmfProperty for RealSmfProperty<'a> {
    fn value(
        &self,
    ) -> Result<Option<Box<dyn SmfValue + '_>>, crucible_smf::ScfError> {
        match &self.p {
            Some(p) => {
                if let Some(v) = p.value()? {
                    Ok(Some(Box::new(RealSmfValue { v })))
                } else {
                    Ok(None)
                }
            }

            None => Ok(None),
        }
    }
}

pub trait SmfValue {
    fn as_string(&self) -> Result<String, crucible_smf::ScfError>;
}

pub struct RealSmfValue<'a> {
    v: crucible_smf::Value<'a>,
}

impl<'a> RealSmfValue<'a> {
    pub fn new(v: crucible_smf::Value<'a>) -> RealSmfValue<'a> {
        RealSmfValue { v }
    }
}

impl<'a> SmfValue for RealSmfValue<'a> {
    fn as_string(&self) -> Result<String, crucible_smf::ScfError> {
        self.v.as_string()
    }
}

pub trait SmfTransaction {
    fn start(&self) -> Result<(), crucible_smf::ScfError>;

    fn property_ensure(
        &self,
        name: &str,
        typ: crucible_smf::scf_type_t,
        val: &str,
    ) -> Result<(), crucible_smf::ScfError>;

    fn commit(
        &self,
    ) -> Result<crucible_smf::CommitResult, crucible_smf::ScfError>;
}

pub struct RealSmfTransaction<'a> {
    t: crucible_smf::Transaction<'a>,
}

impl<'a> RealSmfTransaction<'a> {
    pub fn new(t: crucible_smf::Transaction<'a>) -> RealSmfTransaction<'a> {
        RealSmfTransaction { t }
    }
}

impl<'a> SmfTransaction for RealSmfTransaction<'a> {
    fn start(&self) -> Result<(), crucible_smf::ScfError> {
        self.t.start()
    }

    fn property_ensure(
        &self,
        name: &str,
        typ: crucible_smf::scf_type_t,
        val: &str,
    ) -> Result<(), crucible_smf::ScfError> {
        self.t.property_ensure(name, typ, val)
    }

    fn commit(
        &self,
    ) -> Result<crucible_smf::CommitResult, crucible_smf::ScfError> {
        self.t.commit()
    }
}

#[derive(Debug)]
pub struct MockSmf {
    scope: String,

    // Instance name -> Mock instance entry
    config: Mutex<HashMap<String, MockSmfInstance>>,
}

impl MockSmf {
    #[cfg(test)]
    pub fn new(scope: String) -> MockSmf {
        MockSmf {
            scope,
            config: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    pub fn config_is_empty(&self) -> bool {
        self.config.lock().unwrap().is_empty()
    }
}

impl PartialEq for MockSmf {
    fn eq(&self, other: &Self) -> bool {
        let lhs = self.config.lock().unwrap();
        let rhs = other.config.lock().unwrap();

        self.scope == other.scope && *lhs == *rhs
    }
}

impl SmfInterface for MockSmf {
    fn instances(
        &self,
    ) -> Result<Vec<Box<dyn SmfInstance + '_>>, crucible_smf::ScfError> {
        let config = self.config.lock().unwrap();
        let mut instances: Vec<Box<dyn SmfInstance>> = vec![];

        for value in config.values() {
            instances.push(Box::new(value.clone()));
        }

        Ok(instances)
    }

    fn get_instance(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfInstance + '_>>, crucible_smf::ScfError> {
        let config = self.config.lock().unwrap();
        if let Some(value) = config.get(name) {
            Ok(Some(Box::new(value.clone())))
        } else {
            Ok(None)
        }
    }

    fn add_instance(
        &self,
        name: &str,
    ) -> Result<Box<dyn SmfInstance + '_>, crucible_smf::ScfError> {
        let mut config = self.config.lock().unwrap();

        config.insert(
            name.to_string(),
            MockSmfInstance::new(
                name.to_string(),
                format!("{}:{}", self.scope, name),
            ),
        );

        drop(config);

        Ok(self.get_instance(name)?.unwrap())
    }

    /// When testing to see if running `apply_smf` based on the on-disk
    /// datafile, disabled stuff remains in the current SMF interface and will
    /// cause PartialEq comparison to fail. Prune that here so that comparison
    /// can be done.
    #[cfg(test)]
    fn prune(&self) {
        let mut config = self.config.lock().unwrap();
        let pairs: Vec<(String, MockSmfInstance)> = config.drain().collect();
        for (key, value) in pairs {
            if value.inner.lock().unwrap().enabled {
                config.insert(key, value);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct MockSmfInstanceInner {
    enabled: bool,
    temporary: bool,
    property_groups: HashMap<String, MockSmfPropertyGroup>,
}

impl MockSmfInstanceInner {
    pub fn get_property_group(
        &self,
        name: &str,
    ) -> Option<MockSmfPropertyGroup> {
        self.property_groups.get(name).cloned()
    }

    pub fn add_property_group(
        &mut self,
        name: &str,
        pg_type: &str,
    ) -> MockSmfPropertyGroup {
        self.property_groups
            .entry(name.to_string())
            .or_insert(MockSmfPropertyGroup::new(
                name.to_string(),
                pg_type.to_string(),
            ))
            .clone()
    }
}

#[derive(Clone, Debug)]
pub struct MockSmfInstance {
    name: String,
    fmri: String,
    inner: Arc<Mutex<MockSmfInstanceInner>>,
}

impl MockSmfInstance {
    pub fn new(name: String, fmri: String) -> MockSmfInstance {
        MockSmfInstance {
            name,
            fmri,
            inner: Arc::new(Mutex::new(MockSmfInstanceInner::default())),
        }
    }
}

impl PartialEq for MockSmfInstance {
    fn eq(&self, other: &Self) -> bool {
        let lhs = self.inner.lock().unwrap();
        let rhs = other.inner.lock().unwrap();

        self.name == other.name && self.fmri == other.fmri && *lhs == *rhs
    }
}

impl SmfInstance for MockSmfInstance {
    fn name(&self) -> Result<String, crucible_smf::ScfError> {
        Ok(self.name.clone())
    }

    fn fmri(&self) -> Result<String, crucible_smf::ScfError> {
        Ok(self.fmri.clone())
    }

    fn states(
        &self,
    ) -> Result<
        (Option<crucible_smf::State>, Option<crucible_smf::State>),
        crucible_smf::ScfError,
    > {
        // TODO is this necessary for testing? it's only used for debug print
        // right now
        Ok((None, None))
    }

    fn disable(&self, temporary: bool) -> Result<(), crucible_smf::ScfError> {
        let mut inner = self.inner.lock().unwrap();
        inner.enabled = false;
        inner.temporary = temporary;

        Ok(())
    }

    fn enable(&self, temporary: bool) -> Result<(), crucible_smf::ScfError> {
        let mut inner = self.inner.lock().unwrap();
        inner.enabled = true;
        inner.temporary = temporary;

        Ok(())
    }

    fn enabled(&self) -> bool {
        self.inner.lock().unwrap().enabled
    }

    fn get_pg(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfPropertyGroup + '_>>, crucible_smf::ScfError>
    {
        let inner = self.inner.lock().unwrap();
        if let Some(pg) = inner.get_property_group(name) {
            Ok(Some(Box::new(pg)))
        } else {
            Ok(None)
        }
    }

    fn add_pg(
        &self,
        name: &str,
        pgtype: &str,
    ) -> Result<Box<dyn SmfPropertyGroup + '_>, crucible_smf::ScfError> {
        let mut inner = self.inner.lock().unwrap();
        let pg = inner.add_property_group(name, pgtype);
        Ok(Box::new(pg))
    }

    fn get_snapshot(
        &self,
        _name: &str,
    ) -> Result<Option<Box<dyn SmfSnapshot + '_>>, crucible_smf::ScfError> {
        // XXX this probably isn't how it works?
        let inner = self.inner.lock().unwrap();
        Ok(Some(Box::new(MockSmfSnapshot::new(
            inner.property_groups.clone(),
        ))))
    }
}

#[derive(Debug, PartialEq)]
pub struct MockSmfSnapshot {
    property_groups: HashMap<String, MockSmfPropertyGroup>,
}

impl MockSmfSnapshot {
    pub fn new(
        property_groups: HashMap<String, MockSmfPropertyGroup>,
    ) -> MockSmfSnapshot {
        MockSmfSnapshot { property_groups }
    }
}

impl SmfSnapshot for MockSmfSnapshot {
    fn get_pg(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfPropertyGroup + '_>>, crucible_smf::ScfError>
    {
        if let Some(pg) = self.property_groups.get(name) {
            Ok(Some(Box::new(pg.clone())))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug)]
pub struct MockSmfPropertyGroup {
    pg_name: String,
    pgtype: String,
    properties: Arc<Mutex<HashMap<String, MockSmfProperty>>>,
}

impl MockSmfPropertyGroup {
    pub fn new(pg_name: String, pgtype: String) -> MockSmfPropertyGroup {
        MockSmfPropertyGroup {
            pg_name,
            pgtype,
            properties: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl PartialEq for MockSmfPropertyGroup {
    fn eq(&self, other: &Self) -> bool {
        let lhs = self.properties.lock().unwrap();
        let rhs = other.properties.lock().unwrap();

        self.pg_name == other.pg_name
            && self.pgtype == other.pgtype
            && *lhs == *rhs
    }
}

impl SmfPropertyGroup for MockSmfPropertyGroup {
    fn get_property(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn SmfProperty + '_>>, crucible_smf::ScfError> {
        let properties = self.properties.lock().unwrap();
        if let Some(value) = properties.get(name) {
            Ok(Some(Box::new(value.clone())))
        } else {
            Ok(None)
        }
    }

    fn transaction(
        &self,
    ) -> Result<Box<dyn SmfTransaction + '_>, crucible_smf::ScfError> {
        Ok(Box::new(MockSmfTransaction::new(self.properties.clone())))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MockSmfProperty {
    name: String,
    value: Option<String>,
}

impl MockSmfProperty {
    // XXX don't store scf_type_t, can't derive Eq
    pub fn new(
        name: String,
        _typ: crucible_smf::scf_type_t,
        value: Option<String>,
    ) -> MockSmfProperty {
        MockSmfProperty { name, value }
    }
}

impl SmfProperty for MockSmfProperty {
    fn value(
        &self,
    ) -> Result<Option<Box<dyn SmfValue + '_>>, crucible_smf::ScfError> {
        if let Some(value) = &self.value {
            Ok(Some(Box::new(MockSmfValue {
                value: value.clone(),
            })))
        } else {
            Ok(None)
        }
    }
}

pub struct MockSmfValue {
    value: String,
}

impl SmfValue for MockSmfValue {
    fn as_string(&self) -> Result<String, crucible_smf::ScfError> {
        Ok(self.value.clone())
    }
}

pub struct MockSmfTransaction {
    properties: Arc<Mutex<HashMap<String, MockSmfProperty>>>,
    ensured: Mutex<HashMap<String, MockSmfProperty>>,
}

impl MockSmfTransaction {
    pub fn new(
        properties: Arc<Mutex<HashMap<String, MockSmfProperty>>>,
    ) -> MockSmfTransaction {
        MockSmfTransaction {
            properties,
            ensured: Mutex::new(HashMap::new()),
        }
    }
}

impl SmfTransaction for MockSmfTransaction {
    fn start(&self) -> Result<(), crucible_smf::ScfError> {
        Ok(())
    }

    fn property_ensure(
        &self,
        name: &str,
        typ: crucible_smf::scf_type_t,
        val: &str,
    ) -> Result<(), crucible_smf::ScfError> {
        let mut ensured = self.ensured.lock().unwrap();

        if let Some(p) = ensured.get_mut(name) {
            *p = MockSmfProperty::new(
                name.to_string(),
                typ,
                Some(val.to_string()),
            );
        } else {
            ensured.insert(
                name.to_string(),
                MockSmfProperty::new(
                    name.to_string(),
                    typ,
                    Some(val.to_string()),
                ),
            );
        }

        Ok(())
    }

    fn commit(
        &self,
    ) -> Result<crucible_smf::CommitResult, crucible_smf::ScfError> {
        let ensured = self.ensured.lock().unwrap();
        let mut properties = self.properties.lock().unwrap();

        for (name, property) in &*ensured {
            if let Some(p) = properties.get_mut(name) {
                *p = property.clone();
            } else {
                properties.insert(name.clone(), property.clone());
            }
        }

        Ok(crucible_smf::CommitResult::Success)
    }
}
