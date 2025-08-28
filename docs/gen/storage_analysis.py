from src.config import get_con


def storage_analysis():
    # look at the semantic types of sql storm and stack overflow and check how much data is stored in there

    con = get_con(read_only=True)

