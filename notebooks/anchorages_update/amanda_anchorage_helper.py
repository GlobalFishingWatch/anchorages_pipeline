# -*- coding: utf-8 -*-
# %%
from amanda_notebook_bq_helper import *
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import sys
sys.path.append(os.path.abspath('/Users/alohmann/Library/CloudStorage/Dropbox/GFW/github/amanda/helper_code'))
from amanda_map_helper import *

from s2sphere import CellId, Cell, LatLng
import folium
from folium.features import GeoJson
import pandas as pd

# %%
from branca.element import Template, MacroElement

def add_categorical_legend(m, title, color_map, label_map = None):
    # color_map: dict[label -> css_color]
    if label_map is None:
        items_html = "".join(
            f"<li><span style='background:{color};'></span>{label}</li>"
            for label, color in color_map.items()
        )
    else:
        items_html = "".join(
            f"<li><span style='background:{color};'></span>{label_map.get(label, label)}</li>"
            for label, color in color_map.items()
        )

    template = f"""
    {{% macro html(this, kwargs) %}}
    <div id='maplegend' class='maplegend' 
         style='position: absolute; z-index:9999; border:2px solid #bbb; 
                background-color: rgba(255, 255, 255, 0.9); border-radius:6px; 
                padding: 10px; font-size:14px; left: 100px; top: 20px;'>
      <div class='legend-title'>{title}</div>
      <div class='legend-scale'>
        <ul class='legend-labels'>
          {items_html}
        </ul>
      </div>
    </div>
    <style type='text/css'>
      .maplegend .legend-title {{ text-align:left; margin-bottom:8px; font-weight:bold; font-size:90%; }}
      .maplegend .legend-scale ul {{ margin:0; padding:0; list-style:none; }}
      .maplegend .legend-scale ul li {{ line-height:18px; margin-bottom:4px; }}
      .maplegend ul.legend-labels li span {{
        display:inline-block; height:12px; width:18px; margin-right:6px;
        border:1px solid #999; opacity:0.7;
      }}
    </style>
    {{% endmacro %}}
    """
    macro = MacroElement()
    macro._template = Template(template)
    m.get_root().add_child(macro)

from typing import Dict, Iterable, Tuple, Optional
import pandas as pd
import folium
from folium import GeoJson
from folium.features import GeoJsonTooltip

# Assumes you have:
# from s2geometry import CellId, Cell
# from s2geometry import LatLng
# and a helper: s2id_to_latlon(s2id) -> (lat, lon)

def map_s2_anchorages(
    target_anchorages: pd.DataFrame,
    *,
    zoom_start: int = 18,
    show_labels: bool = False,
    fit_bounds: bool = True,
    color_map: Optional[Dict[str, str]] = None,
    label_map: Optional[Dict[str, str]] = None,
    legend_title: str = "Source"
) -> folium.Map:
    """
    Build a Folium map of S2 cells with centroids and optional label markers.

    Parameters
    ----------
    target_anchorages : pd.DataFrame
        Must contain at least 's2id'. Optional: 'label', 'sublabel', 'source'.
        If 's2lat'/'s2lon' are missing, they will be computed via s2id_to_latlon.
    zoom_start : int
        Initial zoom level if not fitting bounds or for first render.
    show_labels : bool
        Whether the label overlay starts visible (checked) in the layer control.
    fit_bounds : bool
        Auto-fit map to all cell coordinates and centroids.
    color_map : dict[str, str] | None
        Map of source -> color (CSS color/hex). Unmapped sources fall back to 'gray'.

    Returns
    -------
    folium.Map
    """
    if target_anchorages is None or len(target_anchorages) == 0:
        raise ValueError("target_anchorages is empty; nothing to map.")

    df = target_anchorages.copy()

    df["s2lat"], df["s2lon"] = zip(*df["s2id"].map(s2id_to_latlon))

    # Determine map center from the first cell (stable even if fit_bounds=True)
    first_cell = Cell(CellId.from_token(df["s2id"].iloc[0]))
    first_center = LatLng.from_point(first_cell.get_center())
    m = folium.Map(
        location=[first_center.lat().degrees, first_center.lng().degrees],
        zoom_start=zoom_start,
        prefer_canvas=True,
        control_scale=True,
        tiles=None  # start with no base layer
    )

    # Add Esri satellite imagery (will be the default)
    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri',
        name='Esri Satellite',
        overlay=False,
        control=True
    ).add_to(m)

    # Add OpenStreetMap as an alternate base layer
    folium.TileLayer(
        'OpenStreetMap',
        name='OpenStreetMap',
        overlay=False,
        control=True
    ).add_to(m)

    # ---- Build dynamic color map if not provided ----
    if color_map is None:
        unique_sources = df["source"].dropna().unique() if "source" in df.columns else ["no_source"]

        # use a color palette (cycle if there are more sources than colors)
        base_colors = [
            "blue", "green", "purple", "red", "orange",
            "teal", "brown", "pink", "olive"
        ]

        color_map = {
            src: base_colors[i % len(base_colors)]
            for i, src in enumerate(unique_sources)
        }

        # fallback color if any feature lacks a valid source
        color_map["no_source"] = "lightgray"

    # Build GeoJSON features + track bounds as we go
    features: list[dict] = []
    lat_all: list[float] = []
    lon_all: list[float] = []

    for row in df.itertuples(index=False):
        s2id = getattr(row, "s2id")
        label = getattr(row, "label", "NULL") if hasattr(row, "label") else "NULL"
        sublabel = getattr(row, "sublabel", "NULL") if hasattr(row, "sublabel") else "NULL"
        src = getattr(row, "source", "no_source") if hasattr(row, "source") else "NULL"

        # Try constructing the S2 cell; skip invalids
        try:
            cell = Cell(CellId.from_token(s2id))
        except Exception:
            # If you prefer hard-fail: raise ValueError(f"Invalid s2id token: {token}")
            # For robustness, we just skip bad tokens.
            continue

        # Build the cell polygon ring (lon, lat) and close it
        ring: list[Tuple[float, float]] = []
        for i in range(4):
            v = cell.get_vertex(i)
            ll = LatLng.from_point(v)
            ring.append((ll.lng().degrees, ll.lat().degrees))
            lon_all.append(ll.lng().degrees)
            lat_all.append(ll.lat().degrees)
        ring.append(ring[0])

        # Track centroids too for fit_bounds
        lat_all.append(getattr(row, "s2lat"))
        lon_all.append(getattr(row, "s2lon"))

        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [list(ring)]},
            "properties": {
                "label":label,
                "sublabel":sublabel,
                "source": src if pd.notna(src) else "no_source",
                "s2id": s2id
            }
        })

    # Build FeatureCollection
    fc = {"type": "FeatureCollection", "features": features, "tolerance": 0}

    # Style features by 'source'
    def style_fn(feature: dict) -> dict:
        src = feature.get("properties", {}).get("source", "no_source")
        color = color_map.get(src, "gray")
        return {
            "color": color,      # stroke
            "weight": 2,
            "fill": True,
            "fillColor": color,
            "fillOpacity": 0.2,
        }

    gj = GeoJson(
        fc,
        name="S2 Cells",
        style_function=style_fn,
        tooltip=GeoJsonTooltip(
            fields=["label","sublabel", "source","s2id"],
            aliases=["Label:", "Sublabel:", "Source:","s2 cell:"],
            sticky=False,
            labels=True
        ),
    ).add_to(m)

    # Centroids layer (color by source)
    centroid_layer = folium.FeatureGroup(name="S2 Centroid")
    for row in df.itertuples(index=False):
        src = getattr(row, "source", "ais_detected") if hasattr(row, "source") else "ais_detected"
        color = color_map.get(src, "gray")
        lat = getattr(row, "s2lat")
        lon = getattr(row, "s2lon")
        folium.CircleMarker(
            location=[lat, lon],
            radius=2,
            color=color,
            fill=True,
            fill_opacity=0.7,
        ).add_to(centroid_layer)
    centroid_layer.add_to(m)

    # High-zoom label markers (hidden by default unless show_labels=True)
    high_zoom_labels = folium.FeatureGroup(name="Label / Sublabel", show=show_labels)
    for row in df.itertuples(index=False):
        label = getattr(row, "label", "NULL") if hasattr(row, "label") else "NULL"
        sublabel = getattr(row, "sublabel", "NULL") if hasattr(row, "sublabel") else "NULL"
        center = [getattr(row, "s2lat"), getattr(row, "s2lon")]
        folium.Marker(
            location=center,
            icon=folium.DivIcon(
                html=f"""
                <div style="
                    display:inline-block;font-size:15px;color:black;
                    background-color:rgba(255,255,255,0.9);padding:4px 8px;border-radius:4px;
                    text-align:center;transform:translate(-50%,-50%);position:relative;
                    min-width:50px;white-space:nowrap;box-shadow:0 0 2px rgba(0,0,0,0.2);
                ">{label}<br>{sublabel}</div>
                """
            ),
        ).add_to(high_zoom_labels)
    high_zoom_labels.add_to(m)

    # Optionally fit bounds to everything we added
    if fit_bounds and lat_all and lon_all:
        m.fit_bounds([[min(lat_all), min(lon_all)], [max(lat_all), max(lon_all)]])

    folium.LayerControl(collapsed=False).add_to(m)

    add_categorical_legend(m, legend_title, color_map, label_map)
    return m


# %%
def cellid_from_token(token: str) -> CellId:
    """Parse an S2CellId token (hex without trailing zeros) into a CellId."""
    token = token.lower().strip()
    if not (1 <= len(token) <= 16):
        raise ValueError("S2 token must be 1..16 hex chars")
    val = int(token, 16)
    # pad zeros on the RIGHT to reach 16 hex chars => shift left by 4 bits per missing hex digit
    shift = 4 * (16 - len(token))
    return CellId(val << shift)

def s2_center_distance_meters(id1_hex: str, id2_hex: str) -> float:
    """Distance between centroids of two s2 cells, in meters"""
    EARTH_RADIUS_M = 6_371_000
    c1 = cellid_from_token(id1_hex)
    c2 = cellid_from_token(id2_hex)
    ll1 = c1.to_lat_lng()
    ll2 = c2.to_lat_lng()
    return ll1.get_distance(ll2).radians * EARTH_RADIUS_M

def clean_overrides(df, duplicate_option = 'nothing'):
    duplicate_options = ['keep_last', 'combine_with_ampersand','nothing']
    if duplicate_option not in duplicate_options:
        raise Exception(f"{duplicate_option} not a valid duplicate_option")

    # fix messed up s2ids
    messed_up_s2id_count = 0
    for idx, row in df.iterrows():
        s2id = str(row['s2id'])
        if ('+' in s2id) or (len(s2id) != 8):
            df.loc[idx, 's2id'] = s2_anchorage_style(row['latitude'], row['longitude'])
            messed_up_s2id_count = messed_up_s2id_count+1
    print(f"Fixed {messed_up_s2id_count} messed up s2ids")

    # handle duplicates

    old_len = len(df)

    dupes = df[df.duplicated(subset='s2id', keep=False)]
    print(f"There are {len(dupes)} duplicate rows across {len(np.unique(dupes['s2id']))} s2ids")
    if len(dupes) > 0:
        print(f"\n ** START DUPLICATE INFO ** ")
        
        dupes1 = dupes[['s2id','latitude','longitude']]

        conflicting_ids = (
            dupes1.groupby('s2id')
            .nunique(dropna=False)          # count distinct values per column
            .gt(1)                          # flag columns with >1 unique value
            .any(axis=1)                    # True if any column differs
        )
        conflicting_ids = conflicting_ids[conflicting_ids].index

        # Filter and order
        conflicting_rows = dupes1[dupes1['s2id'].isin(conflicting_ids)]
        print(f"Different lat/lons: {len(conflicting_rows)} duplicate rows with {len(np.unique(conflicting_rows['s2id']))} s2ids")


        dupes1 = dupes[['s2id','label','sublabel','iso3']]

        conflicting_ids = (
            dupes1.groupby('s2id')
            .nunique(dropna=False)          # count distinct values per column
            .gt(1)                          # flag columns with >1 unique value
            .any(axis=1)                    # True if any column differs
        )
        conflicting_ids = conflicting_ids[conflicting_ids].index

        # Filter and order
        conflicting_rows = dupes1[dupes1['s2id'].isin(conflicting_ids)].sort_values(by='s2id')
        print(f"Different labels/sublabels: {len(conflicting_rows)} duplicate rows with {len(np.unique(conflicting_rows['s2id']))} s2ids:")
        print(conflicting_rows)



    if duplicate_option == 'keep_last':
        df = df.drop_duplicates(subset='s2id', keep='last').reset_index(drop=True)
        print(f"Dropped {old_len - len(df)} duplicates")
        print(f"** END DUPLICATE INFO ** \n")
    elif duplicate_option == 'combine_with_ampersand':
        # Filter to duplicated s2ids (includes all occurrences)
        dupes = df[df.duplicated(subset='s2id', keep=False)]

        # Group by s2id so you can loop through each set of duplicates

        n_labels_combined = 0
        n_sublabels_combined = 0

        for s2id, group in dupes.groupby('s2id'):
            max_idx = group.index.max()

            if len(np.unique(group['label'])) > 1:
                s = ''
                for x in np.unique(group['label']):
                    if pd.isna(x):
                        pass
                    elif len(s) == 0:
                        s = x
                    else:
                        s = f"{s} & {x}"
                if len(s) > 0:
                    df.loc[max_idx, 'label'] = s
                    n_labels_combined = n_labels_combined + len(group) - 1

            if group['sublabel'].nunique(dropna=False) > 1: # can handle None
                s = ''
                for x in np.unique(group['sublabel']):
                    if pd.isna(x):
                        pass
                    elif len(s) == 0:
                        s = x
                    else:
                        s = f"{s} & {x}"
                if len(s) > 0:
                    df.loc[max_idx, 'sublabel'] = s
                    n_sublabels_combined = n_sublabels_combined + len(group) - 1

            idxs = list(group.index)
            idxs.remove(max(idxs))
                
            df = df.drop(index=idxs)
            
        df = df.reset_index(drop=True)
        print(f"Handled {old_len - len(df)} duplicates")
        print(f"Rows whose labels were combined: {n_labels_combined}")
        print(f"Rows whose sublabels were combined: {n_sublabels_combined}")
    
    elif duplicate_option == 'nothing':
        dupes = df[df.duplicated(subset='s2id', keep=False)]
        if len(dupes) > 0:
            print(f"WARNING: There are {len(dupes)} duplicated s2ids that were not handled")
        else:
            print(f"There are 0 duplicated s2ids")

    else:
        raise Exception("invalid duplicate option, should be unreachable")
        
    return(df)


# %%
def int_to_s2id(n: int) -> str:
    if not isinstance(n, int):
        raise TypeError("Input must be an integer.")
    s = format(n, 'x')
    return(s[0:8])


# %%
