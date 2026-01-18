"""
Sick day calculation logic for DiyurCalc.
Handles identification of sick day sequences and payment rate determination.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List


def _identify_sick_day_sequences(reports: List[Dict]) -> Dict[date, int]:
    """
    זיהוי רצפי ימי מחלה וקביעת מספר היום ברצף לכל תאריך.

    לפי חוק דמי מחלה:
    - יום ראשון: 0% תשלום
    - ימים 2-3: 50% תשלום
    - מיום 4 והלאה: 100% תשלום

    תאריכים רצופים (כולל ימי מנוחה) נחשבים כרצף אחד.
    הפסקה של יותר מיום אחד מתחילה רצף חדש.

    Args:
        reports: רשימת דיווחים מהדאטבייס

    Returns:
        מילון {תאריך: מספר_יום_מחלה} (1, 2, 3, 4...)
    """
    # איסוף כל התאריכים שיש בהם דיווח מחלה
    sick_dates = set()
    for r in reports:
        shift_name = r.get("shift_name") or ""
        if "מחלה" in shift_name:
            r_date = r.get("date")
            if r_date:
                if isinstance(r_date, datetime):
                    sick_dates.add(r_date.date())
                elif isinstance(r_date, date):
                    sick_dates.add(r_date)

    if not sick_dates:
        return {}

    # מיון לפי תאריך
    sorted_dates = sorted(sick_dates)

    # בניית מילון עם מספר יום לכל תאריך
    sick_day_numbers = {}
    day_in_sequence = 1

    for i, d in enumerate(sorted_dates):
        if i == 0:
            sick_day_numbers[d] = 1
        else:
            prev_date = sorted_dates[i - 1]
            # אם ההפרש הוא יום אחד בדיוק - המשך רצף
            if (d - prev_date).days == 1:
                day_in_sequence += 1
            else:
                # הפסקה - התחלת רצף חדש
                day_in_sequence = 1
            sick_day_numbers[d] = day_in_sequence

    return sick_day_numbers


def get_sick_payment_rate(sick_day_number: int) -> float:
    """
    קביעת אחוז התשלום לפי מספר יום המחלה ברצף.

    Args:
        sick_day_number: מספר היום ברצף (1, 2, 3, 4...)

    Returns:
        אחוז התשלום (0.0, 0.5, או 1.0)
    """
    if sick_day_number == 1:
        return 0.0  # יום ראשון - 0%
    elif sick_day_number <= 3:
        return 0.5  # ימים 2-3 - 50%
    else:
        return 1.0  # מיום 4 והלאה - 100%
