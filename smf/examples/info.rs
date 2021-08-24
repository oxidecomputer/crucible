
use crucible_smf::Result;

fn main() -> Result<()> {
    let scf = crucible_smf::Scf::new()?;

    let mut scopes = scf.scopes()?;
    while let Some(scope) = scopes.next().transpose()? {
        println!("scope: {}", scope.name()?);

        let mut services = scope.services()?;
        while let Some(service) = services.next().transpose()? {
            println!("  service: {}", service.name()?);

            let mut pgs = service.pgs()?;
            while let Some(pg) = pgs.next().transpose()? {
                println!("    pg(s): {}", pg.name()?);
            }

            let mut instances = service.instances()?;
            while let Some(instance) = instances.next().transpose()? {
                println!("    instance: {}", instance.name()?);

                let mut pgs = instance.pgs()?;
                while let Some(pg) = pgs.next().transpose()? {
                    println!("      pg(i): {}", pg.name()?);
                }

                let mut snapshots = instance.snapshots()?;
                while let Some(snapshot) = snapshots.next().transpose()? {
                    println!("      snapshot: {}", snapshot.name()?);

                    let mut pgs = snapshot.pgs()?;
                    while let Some(pg) = pgs.next().transpose()? {
                        println!("        pg(c): {}", pg.name()?);
                    }
                }
            }
        }
    }

    Ok(())
}
