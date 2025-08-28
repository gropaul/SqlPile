def float_matcher():
    prefixes = ['signed', 'unsigned', 'u']
    suffixes = ['unsigned', 'signed', 'u']

    size_types = {
        'Float16': ['float2', 'float16', 'half', 'halfprecision', 'halffloat', 'half_float', 'binary16', 'fp16'],
        'Float32': ['float', 'float4', 'float32', 'real', 'single', 'singleprecision', 'binary32', 'fp32'],
        'Float64': ['double', 'float8', 'float64', 'doubleprecision', 'binary64', 'fp64'],
        'Float80':  ['float80', 'longdouble', 'extendedprecision', 'binary80'],
        'Float128': ['float128', 'quad', 'quadruple', 'binary128', 'fp128'],
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
    print("_FLOAT_CANONICALS = (")
    for pattern, signed, unsigned in canonical_map:
        print(f"    (r'{pattern}', '{signed}', '{unsigned}'),")
    print(")")


if __name__ == "__main__":
    float_matcher()
    # This will print the _FLOAT_CANONICALS tuple in the same format
