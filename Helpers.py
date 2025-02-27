import itertools
from itertools import chain, islice


def turn_list_of_lists_into_string(arr: [[]]) -> str:
    return ','.join('(' + ','.join(row) + ')' for row in arr)


def peek_generator(iterable):
    try:
        first = next(iterable)
    except StopIteration:
        return None
    yield from itertools.chain([first], iterable)


def ichunked(seq, chunksize):
    """Yields items from an iterator in iterable chunks."""
    it = iter(seq)
    while True:
        try:
            yield chain([next(it)], islice(it, chunksize - 1))
        except StopIteration:
            break


def chunked(seq, chunksize):
    """Yields items from an iterator in list chunks."""
    for chunk in ichunked(seq, chunksize):
        yield list(chunk)

def construct_naampad(input_dict: dict) -> str:
    """
    Construct naampad by walking recursively in a nested "parent" dictionary, searching for the "naam" key.
    Concatenates all "naam" values, starting from the top of the nested tree.
    :param input_dict:
    :return: str
    """
    naam_list = []
    while "naam" in input_dict:
        naam_list.insert(0, input_dict["naam"]) # insert at first list index position
        if "parent" in input_dict:
            input_dict = input_dict["parent"]
        else:
            break
    return "/".join(naam_list)  # concatenate naampad, using "/" as a separator character