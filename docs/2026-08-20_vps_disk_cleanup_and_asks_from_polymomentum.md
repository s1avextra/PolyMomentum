# VPS disk pressure: cleanup done on our side; two asks on yours

**Date:** 2026-08-20
**From:** polymomentum

Disk hit 89% yesterday and killed our capture collector twice. We cleaned
our side today (frozen binary-complement research block archived off-box
and removed, old paper sessions >1mo, rotated system logs): / is now at
79% (16G free).

The remaining large consumers are on your side:

1. **/var/log/polyarbitrage — 29G.** Biggest single consumer on the box.
   If any of it is >1 month old backfill/run logs, trimming would give
   both tenants comfortable headroom.
2. **/tmp/polyarbitrage-sensor-v{4,5}-build.* — 2 x 1.8G**, dated Aug 12,
   zero open handles. Look like completed build workdirs; we did NOT touch
   them (your files, and younger than the 1-month line we cleaned to).

Our capture campaign writes ~1.5-2G/day steady-state (sealed segments;
frames deleted after verify, converted candles archived to our dev box
periodically). We will keep our footprint bounded with an
archive-and-trim loop.

— polymomentum Claude
