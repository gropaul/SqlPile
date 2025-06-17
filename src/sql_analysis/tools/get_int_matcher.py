


def matcher():
    import re

    prefixes = ['signed', 'unsigned', 'u']
    suffixes = ['unsigned', 'signed', 'u']

    size_types = {
        'Int8': ['int1', 'int8', 'tinyint', 'bit'],
        'Int16': ['int2', 'int16', 'smallint', 'smallserial'],
        'Int24': ['int3', 'int24', 'mediumint'],
        'Int32': ['int', 'int4', 'int32', 'integer', 'serial'],
        'Int64': ['int8', 'int64', 'bigint', 'bigserial', 'long'],
    }

    def generate_aliases(base):
        """Generate all prefixed/suffixed aliases."""
        aliases = {base}  # start with the base name itself
        for p in prefixes:
            aliases.add(p + base)
        for s in suffixes:
            aliases.add(base + s)
        return aliases

    canonical_map = []

    for signed_name, base_names in size_types.items():
        unsigned_name = 'U' + signed_name
        all_signed = set()
        all_unsigned = set()

        for base in base_names:
            aliases = generate_aliases(base)
            for alias in aliases:
                norm = alias.lower()
                if (
                        norm.startswith('unsigned') or norm.startswith('u') or
                        norm.endswith('unsigned') or norm.endswith('u')
                ):
                    all_unsigned.add(norm)
                else:
                    all_signed.add(norm)

        # Build regex pattern
        pattern = r'\b(' + '|'.join(sorted(all_signed | all_unsigned)) + r')\b'
        canonical_map.append((pattern, signed_name, unsigned_name))

    # Display the result like a Python tuple literal
    print("_CANONICALS = (")
    for pattern, signed, unsigned in canonical_map:
        print(f"    (r'{pattern}', '{signed}', '{unsigned}'),")
    print(")")


if __name__ == "__main__":
    matcher()
    # This will print the _CANONICALS tuple in the desired format
    # You can redirect this output to a file or use it directly in your code