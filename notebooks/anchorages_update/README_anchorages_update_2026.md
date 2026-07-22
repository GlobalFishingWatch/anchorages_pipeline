# Anchorages Update 2026 README
Amanda Lohmann 2026-07-21

In 2026, we updated the named_anchorages table for pipe5. We added anchorages for VMS (see "VMS country port lists" below), anchorages from Vietnam

## VMS country port lists
- [Brazil](https://globalfishingwatch.slack.com/archives/C04JP2ZCZB4/p1760460179813499?thread_ts=1760453815.652139&cid=C04JP2ZCZB4)
- [Chile](https://globalfishingwatch.slack.com/archives/CHBNB2JAE/p1759928166941019?thread_ts=1759861535.833019&cid=CHBNB2JAE)
-- Tyler extracted these from the [excel doc](https://globalfishingwatch.slack.com/archives/CGXV6UE0Y/p1760723520889259)
-- Amanda swapped the label and sublabel so that label is the PUNTO DE DESEMBARQUE and sublabel is the COMUNA , to be consistent with anchorage labeling where the sublabel is the larger region and the label is the name of the port
- [Costa Rica, Panama, Ecuador](https://globalfishingwatch.slack.com/archives/C04JP2ZCZB4/p1761087107896789?thread_ts=1760737571.202709&cid=C04JP2ZCZB4)

[GFW map workspace from Claudino](https://globalfishingwatch.org/map/index?start=2025-07-18T00%3A00%3A00.000Z&end=2025-10-18T00%3A00%3A00.000Z&longitude=-38.31363350721725&latitude=23.88280507765543&zoom=2.6708918770682057&dvIn[0][id]=context-layer-eez&dvIn[0][cfg][vis]=true&dvIn[1][id]=user-points-public-vms-anchorages-panama-1761080063392-1761086849591&dvIn[1][dvId]=~0&dvIn[1][cfg][clr]=%23F09300&dvIn[1][dsC][0][dsId]=~1&dvIn[1][dsC][0][ept]=~2&dvIn[1][dsC][0][pms][0][id]=id&dvIn[1][dsC][0][pms][0][val]=~1&dvIn[2][id]=user-points-public-vms-anchorages-costa-rica-1761080432858-1761086839328&dvIn[2][dvId]=~0&dvIn[2][cfg][clr]=%2333B679&dvIn[2][cfg][vis]=true&dvIn[2][dsC][0][dsId]=~3&dvIn[2][dsC][0][ept]=~2&dvIn[2][dsC][0][pms][0][id]=id&dvIn[2][dsC][0][pms][0][val]=~3&dvIn[3][id]=user-points-public-vms-anchorages-ecuador-1761085442363-1761086831834&dvIn[3][dvId]=~0&dvIn[3][cfg][clr]=%23F95E5E&dvIn[3][cfg][vis]=true&dvIn[3][dsC][0][dsId]=~4&dvIn[3][dsC][0][ept]=~2&dvIn[3][dsC][0][pms][0][id]=id&dvIn[3][dsC][0][pms][0][val]=~4&dvIn[4][id]=port-locations__1761086817238&dvIn[4][category]=context&dvIn[4][dvId]=ports&dvIn[4][cfg][clr]=%239AEEFF&dvIn[4][cfg][vis]=false&dvIn[5][id]=vms&dvIn[5][cfg][vis]=false&dvIn[6][id]=ais&dvIn[6][cfg][vis]=false&bDV&tk[0]=default-points-layer&tk[1]=public-vms-anchorages-panama-1761080063392&tk[2]=context-tiles&tk[3]=public-vms-anchorages-costa-rica-1761080432858&tk[4]=public-vms-anchorages-ecuador-1761085442363)

## Additional added anchorages
- [Vietnam](https://globalfishingwatch.slack.com/archives/CHBNB2JAE/p1771838107165169?thread_ts=1770751536.878719&cid=CHBNB2JAE)
- [Tarout Island](https://globalfishingwatch.slack.com/archives/CHBNB2JAE/p1772058467571169?thread_ts=1771963391.744739&cid=CHBNB2JAE)

## Steps for generating the new named_anchorages table

Note - I renamed `data/port_lists/vms_anchorage_overrides.csv` to `data/port_lists/brazil_original_overrides.csv` , since that file only has the Brazil anchorages from Claudino’s scratch

1. `parse_country_overrides.py` notebook to parse and save anchorage S2 cells from provided points
    1. Calculate S2 cells that contain the provided points.
    2. Remove any duplicate S2 cell rows; if an S2 cell has multiple labels or sublabels, combine them with an ‘&’ (so if an S2 cell has one entry with label “Port A” and another entry with label “Port B”, combine them into one row with label “Port A & Port B”
    3. Save as `data/port_lists/{country_name}_vms_overrides.csv` 
2. `combine_overrides.py` notebook to combine individual country anchorages plus AIS anchorages and save as `anchorage_overrides.csv`
    1. Remove duplicates from `anchorage_overrides.csv`. If an S2 cell appears multiple times in this file, keep only the last entry (the entry closest to the bottom of the file)
    2. Add the `{country_name}_vms_overrides.csv` entries into `anchorage_overrides.csv`
    3. If a `{country_name}_vms_overrides.csv` has an S2 cell that’s also in the preexisting `anchorage_overrides.csv` , the entry in anchorage_overrides is deleted so only the VMS country label is applied
    4. Of the 8 countries currently present (Brazil, Chile, Ecuador, Panama, Costa Rica, Palau, PNG, and Montenegro), only PNG and Costa Rica have entries of ports in other countries. So I just ordered the joins so that if an S2 cell is in both another country’s file and in Costa Rica’s or PNG’s, the Costa Rica/PNG entry gets dropped. 
        1. TODO: Could make this more explicit so that new countries can be added without having to think about this
3. `named_anchorages_run.py` notebook then runs the code to name the anchorages

- `qa_map_new_anchorages.py` notebook compares old vs. new named_anchorages and lists added/removed anchorages

## Updated anchorage table for pipe5

This information is correct as of 2026-07-21. There is no plan to make further changes for pipe5 unless bugs are discovered.

The new run created `world-fishing-827.scratch_amanda_ttl_120.named_anchorages_v20260302` which will be the anchorage table used in pipe5.

For a list of anchorages that have been added or removed between pipe3/pipe4 and pipe5 named_anchorages version, see `notebooks/anchorages_update/added_or_removed_s2ids_v20260302.csv`

### Anchorages removed from previous version

There are also 4 anchorages which we removed - will list as s2id, lat, lon, label, iso:

The only anchorage in the pipe3/pipe4 `global-fishing-watch.anchorages.named_anchorages` that is NOT in the new named_anchorages table is an erroneous location:

- 8efe7543, 11.8423, -78.9995, Pointe Noire, COG *Coordinates entered incorrectly and point to the Caribbean, not Congo*

There are also 3 anchorages that had already been removed from pipe3/pipe4 `global-fishing-watch.anchorages.named_anchorages`, but that appear to have still been in the data when the previous distance-from-port raster was generated. This time around they were not in the data when the new distance-from-port raster was generated. These are:

- 50000001, 89.9967, -135.0 *Artic Ocean*
- 5aaaaaab, 89.9967, 135.0 *Arctic Ocean*
- 140edc01, 24.4307, 24.4321 *Libya*