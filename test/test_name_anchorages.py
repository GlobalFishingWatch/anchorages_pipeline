# -*- coding: utf-8 -*-
import os
import pytest

from anchorages import name_anchorages


class TestNameAnchorages(object):

    def test_normalize(self):
        normalize = name_anchorages.normalize_label
        assert normalize(u'Спецморнефтепорт') == "SPETSMORNEFTEPORT"
        assert normalize(u'Tromsø') == "TROMSO"


