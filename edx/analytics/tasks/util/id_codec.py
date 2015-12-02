"""Defines a reversible way to label an id with a scope and type to make it unique."""

import base64
import numpy as np
import random


def encode_id(scope, id_type, id_value):
    """Encode a scope-type-value tuple into a single ID string."""
    return base64.b32encode('|'.join([scope, id_type, id_value]))


def decode_id(encoded_id):
    """Decode an ID string back to the original scope-type-value tuple."""
    scope, id_type, id_value = base64.b32decode(encoded_id).split('|')
    return scope, id_type, id_value

class PermutationGenerator(object):

    def __init__(self, seed, k, bits):
        self.bits = bits
        self.permutation_matrix = self.random_permutation_matrix(seed, k)

    def int_to_binvec(self, x):
        """Convert x, which must be less than 2**bits, to an np vector of bits 0/1 bits"""
        if x < 0 or x >= 2**self.bits:
            raise ValueError("{} out of range [0, 2**{}]".format(x, self.bits))

        # yay strings
        str_x = bin(x)[2:].zfill(self.bits)
        return np.array([int(b) for b in str_x])

    def binvec_to_int(self, vec):
        """Convert an np array of bits (type int) to a real int"""
        # more string hacks
        return int("".join(map(str,vec)),2)

    def random_permutation_matrix(self, seed, k):
        """Return a random k-by-k permutation matrix using seed."""
        rng = random.Random(seed)
        # where does each bit go?
        mapping = range(k)
        rng.shuffle(mapping)
        # Now make a matrix that does that
        permutation = np.zeros((k,k),dtype=int)
        for i in range(k):
            permutation[i,mapping[i]] = 1
        return permutation

    def permute(self, x):
        """Given int x with bits bits, permute it using the specified bits-by-bits permutation"""
        vec = self.int_to_binvec(x)
        permuted = vec.dot(self.permutation_matrix)
        return self.binvec_to_int(permuted)

    def unpermute(self, x):
        vec = self.int_to_binvec(x)
        permuted = vec.dot(self.permutation_matrix.T)
        return self.binvec_to_int(permuted)

class UserIdRemapperMixin(object):

    permutation_generator = PermutationGenerator(self.seed_value,32,32)

    @classmethod
    def remap_id(cls, id):
        return cls.permutation_generator.permute(int(id))
