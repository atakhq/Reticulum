import unittest

from .hashes import TestSHA256
from .hashes import TestSHA512
from .identity import TestIdentity
from .link import TestLink
from .channel import TestChannel
from .i2p_resilience import TestI2PRetryPolicy, TestI2PTunnelCleanup

if __name__ == '__main__':
    unittest.main(verbosity=2)
