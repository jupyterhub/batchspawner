# Miscellaneous utilities

import random

def random_port_range(low, high):
    """Factory function to select a random port number in the range [low,high].

    Usage: c.BatchSpawner.random_port = random_port_range(low, high)
    """
    # TODO: This does not prevent port number conflicts like
    # jupyterhub/utils.random_port tries to do.  But we actually
    # can't, because we run on a different host, so
    # jupyterhub.utils.random_port actually doesn't work for us.  #58
    # tries to do this better.
    return lambda: random.randint(low, high)
