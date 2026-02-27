#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def geojson_bbox_to_bbox(geojson_coords):
    """
    Convert GeoJSON polygon coordinates into bounding box values.
    
    Args:
        geojson_coords: list of [[[lon, lat], [lon, lat], ...]]
    
    Returns:
        dict with min_lat, max_lat, min_lon, max_lon
    """
    # Flatten list of coordinates
    coords = geojson_coords[0]
    
    lons = [pt[0] for pt in coords]
    lats = [pt[1] for pt in coords]
    
    return {
        "min_lat": min(lats),
        "max_lat": max(lats),
        "min_lon": min(lons),
        "max_lon": max(lons)
    }


# In[ ]:


from s2sphere import CellId, LatLng
def s2_anchorage_style(lat: float, lon: float) -> str:
    # Build the level-14 S2 cell containing this point and return the first 8 hex digits as a string
    cid = CellId.from_lat_lng(LatLng.from_degrees(lat, lon)).parent(14)
    # Format the 64-bit id as 16 hex chars and take the first 8 (most significant)
    return f"{cid.id():016x}"[:8]


# In[ ]:


import geopandas as gpd
import contextily as ctx
import matplotlib.pyplot as plt

def s2id_to_latlon(s2id):
    # Handle string (token)
    cell = CellId.from_token(s2id)

    latlng = cell.to_lat_lng()
    return latlng.lat().degrees, latlng.lng().degrees


# In[ ]:





# In[ ]:


def static_point_map(df, lat_col = None, lon_col = None, label='Points', title=''):
    possible_lat_cols = ['lat', 'latitude']
    possible_lon_cols = ['lon', 'longitude']

    # Identify latitude column
    if lat_col is None:
      for c in possible_lat_cols:
          if c in df.columns:
              lat_col = c
              break
    if lat_col is None:
        raise Exception("Latitude column could not be identified")

    # Identify longitude column
    if lon_col is None:
      for c in possible_lon_cols:
          if c in df.columns:
              lon_col = c
              break
    if lon_col is None:
        raise Exception("Longitude column could not be identified")

    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs="EPSG:4326"
    )

    # Convert to Web Mercator (for contextily)
    gdf = gdf.to_crs(3857)

    # Compute map extent with padding
    xmin, ymin, xmax, ymax = gdf.total_bounds
    pad_x = (xmax - xmin) * 0.05
    pad_y = (ymax - ymin) * 0.05
    extent = (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)

    # Plot
    fig, ax = plt.subplots(figsize=(10, 10))

    # ✅ Plot the points here
    gdf.plot(ax=ax, color='red', markersize=20, label=label)

    # Add basemap
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)

    # Adjust view
    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])

    # Tidy up
    ax.set_title(title, fontsize=14)
    ax.set_axis_off()
    ax.legend()

    plt.show()

    return

