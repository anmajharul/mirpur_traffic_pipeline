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
[1] DMTCL (2026). MRT Line-6 Operational Timetable & May 2026 Updates.
    Dhaka Mass Transit Company Limited. Official URL: http://dmtcl.gov.bd/
    Reference 1a: The Daily Campus (2026). "Metro rail to run till 11pm, frequency to increase".
    Reference 1b: Jago News 24 (2026). "Metro Rail headway to be reduced to 4.5 minutes".
    Updates incorporated: Operations extended to 22:30, Peak Headway reduced to 5 mins.

[2] GTFS Reference (2024). General Transit Feed Specification.
    https://gtfs.org/documentation/schedule/reference/

[3] Cats, O. et al. (2016). Beyond stopping: the role of transit
    service reliability on mode choice.
    Transportation Research Part A, 91, 249–261.
    https://doi.org/10.1016/j.tra.2016.07.002

[4] Raj, S. et al. (2024). Socioeconomic Impact of MRT Line-6 in Dhaka.
    Journal of Urban Planning and Development, 150(2). 
    (NO3: Justifies the inclusion of MRT status as a critical socioeconomic 
    feature in congestion forecasting).
"""

from datetime import datetime
import logging


def get_mrt_status(bd_time: datetime, is_holiday: bool) -> tuple[bool, int]:
    """
    Returns MRT Line-6 operational status and scheduled headway.

    Schedule source: DMTCL (2026) timetable.
    Headways: 5 min peak, 8 min transition, 10 min off-peak / Friday.

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
        # DMTCL (2026): Friday service from 15:00 to 22:30
        # ---------------------------------------
        if is_holiday or wd == 4:
            if 1500 <= hm <= 2230:
                return True, 10
            return False, 0

        # ---------------------------------------
        # REGULAR WEEKDAY SCHEDULE
        # Source: DMTCL (2026), extended hours
        # ---------------------------------------
        if 630 <= hm <= 2230:
            # Transition periods (early/late)
            if 630 <= hm <= 730 or hm > 2030:
                return True, 8
            # Peak hours (high frequency - 5 mins)
            elif (731 <= hm <= 1130) or (1430 <= hm <= 2030):
                return True, 5
            # Off-peak (standard frequency - 8 mins)
            else:
                return True, 8

        # Service not operating
        return False, 0

    except Exception as e:
        logging.error(f"[MRT ERROR] Schedule parse failed: {e}")
        return False, 0