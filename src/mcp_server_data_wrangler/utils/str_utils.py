def strip_string(s: str) -> str:
    """
    Strips a string of leading and trailing whitespace.
    """
    return s.strip().replace("\n", "").replace("\r", "")
