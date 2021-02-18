import logging
from datetime import datetime
from typing import Optional, TYPE_CHECKING
from superset.utils import core as utils

from superset.db_engine_specs.base import BaseEngineSpec

logger = logging.getLogger()


class CrateDBBaseEngineSpec(BaseEngineSpec):
    """ Abstract class for Crate 'like' databases """

    engine = "crate"
    engine_name = "CrateDB"

    _time_grain_expressions = {
        None: "{col}",
        "PT1S": "DATE_TRUNC('second', {col})",
        "PT1M": "DATE_TRUNC('minute', {col})",
        "PT1H": "DATE_TRUNC('hour', {col})",
        "P1D": "DATE_TRUNC('day', {col})",
        "P1W": "DATE_TRUNC('week', {col})",
        "P1M": "DATE_TRUNC('month', {col})",
        "P0.25Y": "DATE_TRUNC('quarter', {col})",
        "P1Y": "DATE_TRUNC('year', {col})",
    }

    @classmethod
    def convert_dttm(cls, target_type: str, dttm: datetime) -> Optional[str]:
        tt = target_type.upper()
        if tt == utils.TemporalType.DATE:
            return f"TO_DATE('{dttm.date().isoformat()}', 'YYYY-MM-DD')"
        if tt == utils.TemporalType.TIMESTAMP:
            dttm_formatted = dttm.isoformat(sep=" ", timespec="microseconds")
            return f"""DATE_FORMAT('{dttm_formatted}')"""
        return None
