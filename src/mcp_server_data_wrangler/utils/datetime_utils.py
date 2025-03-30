from datetime import datetime


def str_to_datetime(
    datetime_str: str,
) -> datetime:
    try:
        return datetime.fromisoformat(datetime_str)
    except ValueError:
        pass

    try:
        return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError as e:
        raise ValueError(f"Invalid datetime format: {datetime_str}") from e
