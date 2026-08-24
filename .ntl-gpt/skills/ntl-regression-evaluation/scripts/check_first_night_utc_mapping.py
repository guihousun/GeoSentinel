from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo


def local_to_utc_file_date(local_date: str, local_time: str, timezone: str) -> dict[str, str]:
    date_value = datetime.strptime(local_date, "%Y-%m-%d").date()
    time_value = datetime.strptime(local_time, "%H:%M").time()
    local_dt = datetime.combine(date_value, time_value, tzinfo=ZoneInfo(timezone))
    utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
    return {
        "local_datetime": local_dt.isoformat(),
        "utc_datetime": utc_dt.isoformat(),
        "utc_file_date": utc_dt.strftime("%Y-%m-%d"),
    }


def main() -> None:
    iran = local_to_utc_file_date("2024-02-29", "02:00", "Asia/Tehran")
    assert iran["utc_file_date"] == "2024-02-28", iran
    myanmar = local_to_utc_file_date("2025-03-29", "02:00", "Asia/Yangon")
    assert myanmar["utc_file_date"] == "2025-03-28", myanmar
    assert myanmar["utc_datetime"].startswith("2025-03-28T19:30:00"), myanmar
    print("first_night_utc_mapping_ok")
    print(f"iran_utc_file_date={iran['utc_file_date']}")
    print(f"myanmar_utc_file_date={myanmar['utc_file_date']}")
    print(f"myanmar_utc_datetime={myanmar['utc_datetime']}")


if __name__ == "__main__":
    main()
