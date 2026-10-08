from pipe_anchorages.distance import distance, inf


class InOutEventsBase:
    IN_PORT = "IN_PORT"
    AT_SEA = "AT_SEA"
    STOPPED = "STOPPED"

    in_port_states = (IN_PORT, STOPPED)
    all_states = (IN_PORT, AT_SEA, STOPPED)

    EVT_ENTER = "PORT_ENTRY"
    EVT_EXIT = "PORT_EXIT"
    EVT_STOP = "PORT_STOP_BEGIN"
    EVT_START = "PORT_STOP_END"
    EVT_GAP_BEG = "PORT_GAP_BEGIN"
    EVT_GAP_END = "PORT_GAP_END"

    transition_map = {
        (AT_SEA, AT_SEA): [],
        (AT_SEA, IN_PORT): [EVT_ENTER],
        (AT_SEA, STOPPED): [EVT_ENTER, EVT_STOP],
        (IN_PORT, AT_SEA): [EVT_EXIT],
        (IN_PORT, IN_PORT): [],
        (IN_PORT, STOPPED): [EVT_STOP],
        (STOPPED, AT_SEA): [EVT_START, EVT_EXIT],
        (STOPPED, IN_PORT): [EVT_START],
        (STOPPED, STOPPED): [],
        (None, AT_SEA): [],
        (None, IN_PORT): [],
        (None, STOPPED): [],
    }

    def _is_in_port(self, state, dist):
        if dist is None:
            return False
        if dist <= self.anchorage_entry_dist:
            return True
        elif dist >= self.anchorage_exit_dist:
            return False
        else:
            return state in (self.IN_PORT, self.STOPPED)

    def _is_stopped(self, state, speed):
        if speed <= self.stopped_begin_speed:
            return True
        elif speed >= self.stopped_end_speed:
            return False
        else:
            return state == self.STOPPED

    def _anchorage_distance(self, loc, anchorages):
        closest = None
        min_dist = inf
        for anch in sorted(anchorages, key=lambda x: x.s2id):
            dist = distance(loc, anch.mean_location)
            if dist < min_dist:
                min_dist = dist
                closest = anch
        return closest, min_dist

    def _compute_state(self, is_in_port, is_stopped):
        if is_in_port:
            if is_stopped:
                return self.STOPPED
            else:
                return self.IN_PORT
        else:
            return self.AT_SEA
