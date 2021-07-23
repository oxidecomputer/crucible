To spin up a NBD server to issue work to Crucible, do the following:

1. Start up three separate crucible-downstairs:

    $ cargo run -p crucible-downstairs -- -p 3801 -d "$PWD/disks/d1"
    $ cargo run -p crucible-downstairs -- -p 3802 -d "$PWD/disks/d2"
    $ cargo run -p crucible-downstairs -- -p 3803 -d "$PWD/disks/d3"

1. Start up crucible-nbd-server:

    $ cargo run -p crucible-nbd-server -- -t 127.0.0.1:3801 -t 127.0.0.1:3802 -t 127.0.0.1:3803

1. Connect nbd-client to the crucible-nbd-server:

    $ sudo nbd-client 127.0.0.1 10809 /dev/nbd0
    Warning: the oldstyle protocol is no longer supported.
    This method now uses the newstyle protocol with a default export
    Negotiation: ..size = 0MB
    Connected /dev/nbd0

1. From here, use nbd0 as normal:

    $ sudo mkfs.vfat -F 32 /dev/nbd0
    mkfs.fat 4.2 (2021-01-31)
    WARNING: Number of clusters for 32 bit FAT is less then suggested minimum.

    (if your OS does not auto mount, run "sudo mount /dev/nbd0 /mnt/")

    $ date | tee /media/jwm/9287-806A/date
    Fri 23 Jul 2021 03:33:06 PM EDT

    $ df -h /media/jwm/9287-806A/
    Filesystem      Size  Used Avail Use% Mounted on
    /dev/nbd0p1     472K  1.0K  471K   1% /media/jwm/9287-806A

   You should see crucible-downstairs and crucible-nbd-server activity in the other terminal windows.

1. To clean up:

    $ sudo umount /media/jwm/9287-806A/
    $ sudo nbd-client -d /dev/nbd0

Important: when developing, make sure to disconnect and reconnect nbd-client every time crucible-nbd-server is restarted!

