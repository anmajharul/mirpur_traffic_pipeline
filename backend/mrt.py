"""
mrt.py — Q1 DEFENSIBLE MRT STATUS MODULE
==========================================
Purpose:
- Determine MRT Line-6 (Dhaka Metro) operational status and headway
- Schedule-based categorical feature for XGBoost model
- Deterministic, reproducible, no external API dependency

LIMITATION (document in paper):
    This module provides schedule-based approximation only.
    Real-time GTFS (General Transit Feed Specification) integration
    was not available for Dhaka MRT during the study period.
    Future work should integrate GTFS-RT feed from DMTCL when available.

REFERENCES:
[1] DMTCL (2023). MRT Line-6 Operational Timetable.
    Dhaka Mass Transit Company Limited.
    URL: http://dmtcl.gov.bd/
    Archived: https://web.archive.org/web/2024*/http://dmtcl.gov.bd/

[2] GTFS Reference (2024). General Transit Feed Specification.
    https://gtfs.org/documentation/schedule/reference/

[3] Cats, O. et al. (2016). Beyond stopping: the role of transit
    service reliability on mode choice.
    Transportation Research Part A, 91, 249–261.
    https://doi.org/10.1016/j.tra.2016.07.002
"""

from datetime import datetime
import logging


def get_mrt_status(bd_time: datetime, is_holiday: bool) -> tuple[bool, int]:
    """
    Returns MRT Line-6 operational status and scheduled headway.

    Schedule source: DMTCL (2023) timetable.
    Headways: 8 min peak, 10 min transition, 12 min off-peak / Friday.

    Args:
        bd_time: datetime in Bangladesh Standard Time (UTC+6)
        is_holiday: True if public holiday (caller must determine)

    Returns:
        mrt_active (bool): True if MRT is operating
        headway_minutes (int): Expected minutes between trains (0 if inactive)

    Note:
        is_holiday parameter must be supplied by caller.
        No automatic holiday detection is implemented — caller must
        maintain a holiday calendar or integrate Bangladesh govt schedule.
    """
    try:
        hm = bd_time.hour * 100 + bd_time.minute
        wd = bd_time.weekday()  # 0=Monday … 6=Sunday

        # ---------------------------------------
        # FRIDAY / HOLIDAY SCHEDULE
        # DMTCL (2023): Friday service from 15:30
        # ---------------------------------------
        if is_holiday or wd == 4:
            if 1530 <= hm <= 2140:
                return True, 12
            return False, 0

        # ---------------------------------------
        # REGULAR WEEKDAY SCHEDULE
        # Source: DMTCL (2023), Section 2.1
        # ---------------------------------------
        if 710 <= hm <= 2140:
            # Transition periods (early/late)
            if 710 <= hm <= 730 or hm > 2032:
                return True, 10
            # Peak hours (high frequency)
            elif (731 <= hm <= 1136) or (1425 <= hm <= 2032):
                return True, 8
            # Off-peak (standard frequency)
            else:
                return True, 12

        # Service not operating
        return False, 0

    except Exception as e:
        logging.error(f"[MRT ERROR] Schedule parse failed: {e}")
        return False, 0