// Copyright 2021 Oxide Computer Company

use crucible_smf::{PropertyGroups, Result};

fn fil(n: usize, c: char) -> String {
    let mut s = String::new();
    while s.len() < n {
        s.push(c);
    }
    s
}

fn ind(n: usize) -> String {
    fil(n * 4, ' ')
}

fn dump_pgs(indent: usize, name: &str, mut pgs: PropertyGroups) -> Result<()> {
    while let Some(pg) = pgs.next().transpose()? {
        let mut infostr = format!("type {:?}", pg.type_()?);
        if pg.is_persistent()? {
            infostr += ", persistent";
        } else {
            infostr += ", non-persistent";
        }

        println!("{}{}: {} ({})", ind(indent), name, pg.name()?, infostr);
        let mut properties = pg.properties()?;
        while let Some(prop) = properties.next().transpose()? {
            println!(
                "{}prop: {} ({:?})",
                ind(indent + 1),
                prop.name()?,
                prop.type_()?
            );

            let mut values = prop.values()?;
            while let Some(v) = values.next().transpose()? {
                println!(
                    "{}value: {:?} ({:?}, base {:?})",
                    ind(indent + 2),
                    v.as_string()?,
                    v.type_()?,
                    v.base_type()?
                );
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();

    let scf = crucible_smf::Scf::new()?;
    let scope = scf.scope_local()?;

    let mut services = scope.services()?;
    while let Some(service) = services.next().transpose()? {
        let n = service.name()?;

        if !args.is_empty() && !args.iter().any(|a| n.contains(a)) {
            continue;
        }

        println!("{}", fil(78, '='));
        println!("{}service: {}", ind(0), n);

        dump_pgs(1, "pg(s)", service.pgs()?)?;
        println!();

        let mut instances = service.instances()?;
        while let Some(instance) = instances.next().transpose()? {
            println!("{}", fil(78, '-'));
            println!("{}instance: {}", ind(1), instance.name()?);

            dump_pgs(2, "pg(i)", instance.pgs()?)?;
            println!();

            let mut snapshots = instance.snapshots()?;
            while let Some(snapshot) = snapshots.next().transpose()? {
                println!("{}snapshot: {}", ind(2), snapshot.name()?);

                dump_pgs(3, "pg(c)", snapshot.pgs()?)?;
                println!();
            }

            println!();
        }

        println!();
    }

    Ok(())
}
