import apache_beam as beam

"""
Groups by vessel_id and later order by exit of port visist.
"""
class GroupByVessels(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | self.group_by_vessel()
            | self.sort_by_exit_port()
        )

    def group_by_vessel(self):
        return beam.GroupBy(lambda x: x['vessel_id'])

    def sort_by_exit_port(self):
        # Sort by the exit of the visit
        return beam.MapTuple(lambda k,v: (k, sorted(v, key=lambda x:x['end_timestamp'])))

