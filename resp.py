def parse_resp(data: bytes) -> list[str] | None:
    """Parse a RESP array or inline command into a list of tokens."""
    try:
        lines = data.split(b"\r\n")
        if lines[0].startswith(b"*"):
            count = int(lines[0][1:])
            args, i = [], 1
            for _ in range(count):
                if not lines[i].startswith(b"$"):
                    return None
                i += 1
                args.append(lines[i].decode("utf-8"))
                i += 1
            return args
        else:
            line = lines[0].decode("utf-8").strip()
            return line.split() if line else None
    except Exception:
        return None


def encode(value: str, is_simple: bool) -> bytes:
    if is_simple:
        return f"+{value}\r\n".encode()
    return f"${len(value)}\r\n{value}\r\n".encode()
