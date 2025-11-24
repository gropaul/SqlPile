


def clean_name(name: str) -> str:
    # only - delimiters are allowed in schema names
    name = (name
            .replace('_', '-')
            .replace('/', '-')
            .replace(' ', '-').lower())
    return name