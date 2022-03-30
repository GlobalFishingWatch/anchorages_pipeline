"""
Find Port Information.

Can be used from the command line to apply port information to a csv file of ports.
For example:

    python -m pipe_anchorages.port_info_finder UNNAMED_ANCHORAGES.csv NAMED_ANCHORAGES.csv

Use `python -m pipe_anchorages.port_info_finder --help` to see full options.


"""
from __future__ import absolute_import, print_function, division
import os
import unidecode
import six
from . import dirnames
from .nearest_port import get_port_finder


def mangled_path(x, subdir):
    this_dir = dirnames.this_dir
    return os.path.join(this_dir, 'data', subdir, x)


def normalize_label(lbl):
    if not lbl:
        return None
    lbl = lbl.strip()
    if not lbl:
        return None
    return unidecode.unidecode(six.ensure_text(lbl)).upper()


class PortInfoFinder(object):
    """object that finds the nearest port to a location

    Parameters
    ----------
    port_finder_paths : list of str
        Paths to lists of port labels.
    label_distance_km : float
        Distance within label information is returned
    sublabel_distance_km: float
        Distance within which sublabel information is returned

    The files specified by `port_finder_paths` should be csv files with the
    following fields:
        iso3 : str
            3-letter country code.
        label : str
            General name of port (e.g., "Oakland").
        sublabel : str
            More specific name of anchorage (e.g., "Pier-5"). May be blank.
        latitude : float
        longitude : float

    """

    def __init__(self, port_finder_paths, label_distance_km, sublabel_distance_km):
        self.port_finder_paths = port_finder_paths
        self.label_distance_km = label_distance_km
        self.sublabel_distance_km = sublabel_distance_km

    @classmethod
    def from_config(cls, config):
        paths =  [config['override_path']] + config['port_list_paths']
        return cls(paths, config['label_distance_km'], config['sublabel_distance_km'])

    def find(self, loc):
        """Find info on nearest port

        Parameters
        ----------
        loc : LatLon
            LatLon is a namedtuple (`lat`, `lon`) of float.

        Returns
        -------
        port : Port or None
            Port is a namedtuple (`iso3`, `label`, `sublabel`, `lat`, `lon`).
        source : str or None
            list the port information is from,

        The returned value depends on the distance to the nearest port found in
        the lists specified in `port_finder_paths`.

        distance <= `sublabel_distance_km` : 
            Both `label` and `sublabel` values will be included in `port`.
        `sublabel_distance_km` < distance <= `label_distance_km` :
            Only the `label value will be included in `port`.
         distance > `label_distance_km` :
            None will be returned for `port` and `source`
    
        """
        for path in self.port_finder_paths:
            source = os.path.splitext(os.path.basename(path))[0]
            finder = get_port_finder(mangled_path(path, 'port_lists'))
            port, distance = finder.find_nearest_port_and_distance(loc)
            if distance <= self.sublabel_distance_km:
                return port, source
            elif distance <= self.label_distance_km:
                return port._replace(sublabel=''), source
        return None, None


if __name__ == "__main__":
    import argparse
    import csv
    import yaml
    from .common import LatLon
    from .shapefile_to_iso3 import get_iso3_finder

    default_config_path = os.path.join(dirnames.parent_dir, 'name_anchorages_cfg.yaml')

    parser = argparse.ArgumentParser()
    parser.add_argument('anchorages_path', 
                        help='path to CSV file containing anchorages to assign names to')
    parser.add_argument('destination_path',
                        help='path to write results to')
    parser.add_argument('--config-path', 
                        help='path to configuration file.',
                        default=default_config_path)

    args = parser.parse_args()

    with open(args.config_path) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    finder = PortInfoFinder.from_config(config)
    isofinder = get_iso3_finder(mangled_path('EEZ_land_v2_201410.shp', 'EEZ'))

    with open(args.anchorages_path) as infile, open(args.destination_path, 'w') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, reader.fieldnames + ['iso3', 'label', 'sublabel', 'label_source'])
        writer.writeheader()
        for x in reader:
            loc = LatLon(float(x['lat']), float(x['lon']))
            port, source = finder.find(loc)
            if port is None:
                x['sublabel'] = ''
                if 'top_destination' in x:
                    x['iso3'] = isofinder.iso3(loc.lat, loc.lon)
                    x['label'] = normalize_label(x['top_destination'])
                    x['label_source'] = 'top_destination'
                else:
                    x['iso3'] = ''
                    x['label'] = ''
                    x['label_source'] = ''
            else:
                x['iso3'] = normalize_label(port.iso3)
                x['label'] = normalize_label(port.label)
                x['sublabel'] = normalize_label(port.sublabel)
                x['label_source'] = source
            writer.writerow(x)


