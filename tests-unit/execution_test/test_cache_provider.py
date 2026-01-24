"""Tests for external cache provider API."""

import importlib.util
import pytest
from typing import Optional


def _torch_available() -> bool:
    """Check if PyTorch is available."""
    return importlib.util.find_spec("torch") is not None


from comfy_execution.cache_provider import (
    CacheProvider,
    CacheContext,
    CacheValue,
    register_cache_provider,
    unregister_cache_provider,
    get_cache_providers,
    has_cache_providers,
    clear_cache_providers,
    serialize_cache_key,
    contains_nan,
    estimate_value_size,
    _canonicalize,
)


class TestCanonicalize:
    """Test _canonicalize function for deterministic ordering."""

    def test_frozenset_ordering_is_deterministic(self):
        """Frozensets should produce consistent canonical form regardless of iteration order."""
        # Create two frozensets with same content
        fs1 = frozenset([("a", 1), ("b", 2), ("c", 3)])
        fs2 = frozenset([("c", 3), ("a", 1), ("b", 2)])

        result1 = _canonicalize(fs1)
        result2 = _canonicalize(fs2)

        assert result1 == result2

    def test_nested_frozenset_ordering(self):
        """Nested frozensets should also be deterministically ordered."""
        inner1 = frozenset([1, 2, 3])
        inner2 = frozenset([3, 2, 1])

        fs1 = frozenset([("key", inner1)])
        fs2 = frozenset([("key", inner2)])

        result1 = _canonicalize(fs1)
        result2 = _canonicalize(fs2)

        assert result1 == result2

    def test_dict_ordering(self):
        """Dicts should be sorted by key."""
        d1 = {"z": 1, "a": 2, "m": 3}
        d2 = {"a": 2, "m": 3, "z": 1}

        result1 = _canonicalize(d1)
        result2 = _canonicalize(d2)

        assert result1 == result2

    def test_tuple_preserved(self):
        """Tuples should be marked and preserved."""
        t = (1, 2, 3)
        result = _canonicalize(t)

        assert result[0] == "__tuple__"
        assert result[1] == [1, 2, 3]

    def test_list_preserved(self):
        """Lists should be recursively canonicalized."""
        lst = [{"b": 2, "a": 1}, frozenset([3, 2, 1])]
        result = _canonicalize(lst)

        # First element should be dict with sorted keys
        assert result[0] == {"a": 1, "b": 2}
        # Second element should be canonicalized frozenset
        assert result[1][0] == "__frozenset__"

    def test_primitives_unchanged(self):
        """Primitive types should pass through unchanged."""
        assert _canonicalize(42) == 42
        assert _canonicalize(3.14) == 3.14
        assert _canonicalize("hello") == "hello"
        assert _canonicalize(True) is True
        assert _canonicalize(None) is None

    def test_bytes_converted(self):
        """Bytes should be converted to hex string."""
        b = b"\x00\xff"
        result = _canonicalize(b)

        assert result[0] == "__bytes__"
        assert result[1] == "00ff"

    def test_set_ordering(self):
        """Sets should be sorted like frozensets."""
        s1 = {3, 1, 2}
        s2 = {1, 2, 3}

        result1 = _canonicalize(s1)
        result2 = _canonicalize(s2)

        assert result1 == result2
        assert result1[0] == "__set__"


class TestSerializeCacheKey:
    """Test serialize_cache_key for deterministic hashing."""

    def test_same_content_same_hash(self):
        """Same content should produce same hash."""
        key1 = frozenset([("node_1", frozenset([("input", "value")]))])
        key2 = frozenset([("node_1", frozenset([("input", "value")]))])

        hash1 = serialize_cache_key(key1)
        hash2 = serialize_cache_key(key2)

        assert hash1 == hash2

    def test_different_content_different_hash(self):
        """Different content should produce different hash."""
        key1 = frozenset([("node_1", "value_a")])
        key2 = frozenset([("node_1", "value_b")])

        hash1 = serialize_cache_key(key1)
        hash2 = serialize_cache_key(key2)

        assert hash1 != hash2

    def test_returns_bytes(self):
        """Should return bytes (SHA256 digest)."""
        key = frozenset([("test", 123)])
        result = serialize_cache_key(key)

        assert isinstance(result, bytes)
        assert len(result) == 32  # SHA256 produces 32 bytes

    def test_complex_nested_structure(self):
        """Complex nested structures should hash deterministically."""
        # Note: frozensets can only contain hashable types, so we use
        # nested frozensets of tuples to represent dict-like structures
        key = frozenset([
            ("node_1", frozenset([
                ("input_a", ("tuple", "value")),
                ("input_b", frozenset([("nested", "dict")])),
            ])),
            ("node_2", frozenset([
                ("param", 42),
            ])),
        ])

        # Hash twice to verify determinism
        hash1 = serialize_cache_key(key)
        hash2 = serialize_cache_key(key)

        assert hash1 == hash2

    def test_dict_in_cache_key(self):
        """Dicts passed directly to serialize_cache_key should work."""
        # This tests the _canonicalize function's ability to handle dicts
        key = {"node_1": {"input": "value"}, "node_2": 42}

        hash1 = serialize_cache_key(key)
        hash2 = serialize_cache_key(key)

        assert hash1 == hash2
        assert isinstance(hash1, bytes)
        assert len(hash1) == 32


class TestContainsNan:
    """Test contains_nan utility function."""

    def test_nan_float_detected(self):
        """NaN floats should be detected."""
        assert contains_nan(float('nan')) is True

    def test_regular_float_not_nan(self):
        """Regular floats should not be detected as NaN."""
        assert contains_nan(3.14) is False
        assert contains_nan(0.0) is False
        assert contains_nan(-1.5) is False

    def test_infinity_not_nan(self):
        """Infinity is not NaN."""
        assert contains_nan(float('inf')) is False
        assert contains_nan(float('-inf')) is False

    def test_nan_in_list(self):
        """NaN in list should be detected."""
        assert contains_nan([1, 2, float('nan'), 4]) is True
        assert contains_nan([1, 2, 3, 4]) is False

    def test_nan_in_tuple(self):
        """NaN in tuple should be detected."""
        assert contains_nan((1, float('nan'))) is True
        assert contains_nan((1, 2, 3)) is False

    def test_nan_in_frozenset(self):
        """NaN in frozenset should be detected."""
        assert contains_nan(frozenset([1, float('nan')])) is True
        assert contains_nan(frozenset([1, 2, 3])) is False

    def test_nan_in_dict_value(self):
        """NaN in dict value should be detected."""
        assert contains_nan({"key": float('nan')}) is True
        assert contains_nan({"key": 42}) is False

    def test_nan_in_nested_structure(self):
        """NaN in deeply nested structure should be detected."""
        nested = {"level1": [{"level2": (1, 2, float('nan'))}]}
        assert contains_nan(nested) is True

    def test_non_numeric_types(self):
        """Non-numeric types should not be NaN."""
        assert contains_nan("string") is False
        assert contains_nan(None) is False
        assert contains_nan(True) is False


class TestEstimateValueSize:
    """Test estimate_value_size utility function."""

    def test_empty_outputs(self):
        """Empty outputs should have zero size."""
        value = CacheValue(outputs=[])
        assert estimate_value_size(value) == 0

    @pytest.mark.skipif(
        not _torch_available(),
        reason="PyTorch not available"
    )
    def test_tensor_size_estimation(self):
        """Tensor size should be estimated correctly."""
        import torch

        # 1000 float32 elements = 4000 bytes
        tensor = torch.zeros(1000, dtype=torch.float32)
        value = CacheValue(outputs=[[tensor]])

        size = estimate_value_size(value)
        assert size == 4000

    @pytest.mark.skipif(
        not _torch_available(),
        reason="PyTorch not available"
    )
    def test_nested_tensor_in_dict(self):
        """Tensors nested in dicts should be counted."""
        import torch

        tensor = torch.zeros(100, dtype=torch.float32)  # 400 bytes
        value = CacheValue(outputs=[[{"samples": tensor}]])

        size = estimate_value_size(value)
        assert size == 400


class TestProviderRegistry:
    """Test cache provider registration and retrieval."""

    def setup_method(self):
        """Clear providers before each test."""
        clear_cache_providers()

    def teardown_method(self):
        """Clear providers after each test."""
        clear_cache_providers()

    def test_register_provider(self):
        """Provider should be registered successfully."""
        provider = MockCacheProvider()
        register_cache_provider(provider)

        assert has_cache_providers() is True
        providers = get_cache_providers()
        assert len(providers) == 1
        assert providers[0] is provider

    def test_unregister_provider(self):
        """Provider should be unregistered successfully."""
        provider = MockCacheProvider()
        register_cache_provider(provider)
        unregister_cache_provider(provider)

        assert has_cache_providers() is False

    def test_multiple_providers(self):
        """Multiple providers can be registered."""
        provider1 = MockCacheProvider()
        provider2 = MockCacheProvider()

        register_cache_provider(provider1)
        register_cache_provider(provider2)

        providers = get_cache_providers()
        assert len(providers) == 2

    def test_duplicate_registration_ignored(self):
        """Registering same provider twice should be ignored."""
        provider = MockCacheProvider()

        register_cache_provider(provider)
        register_cache_provider(provider)  # Should be ignored

        providers = get_cache_providers()
        assert len(providers) == 1

    def test_clear_providers(self):
        """clear_cache_providers should remove all providers."""
        provider1 = MockCacheProvider()
        provider2 = MockCacheProvider()

        register_cache_provider(provider1)
        register_cache_provider(provider2)
        clear_cache_providers()

        assert has_cache_providers() is False
        assert len(get_cache_providers()) == 0


class TestCacheContext:
    """Test CacheContext dataclass."""

    def test_context_creation(self):
        """CacheContext should be created with all fields."""
        context = CacheContext(
            prompt_id="prompt-123",
            node_id="node-456",
            class_type="KSampler",
            cache_key=frozenset([("test", "value")]),
            cache_key_bytes=b"hash_bytes",
        )

        assert context.prompt_id == "prompt-123"
        assert context.node_id == "node-456"
        assert context.class_type == "KSampler"
        assert context.cache_key == frozenset([("test", "value")])
        assert context.cache_key_bytes == b"hash_bytes"


class TestCacheValue:
    """Test CacheValue dataclass."""

    def test_value_creation(self):
        """CacheValue should be created with outputs."""
        outputs = [[{"samples": "tensor_data"}]]
        value = CacheValue(outputs=outputs)

        assert value.outputs == outputs


class MockCacheProvider(CacheProvider):
    """Mock cache provider for testing."""

    def __init__(self):
        self.lookups = []
        self.stores = []

    def on_lookup(self, context: CacheContext) -> Optional[CacheValue]:
        self.lookups.append(context)
        return None

    def on_store(self, context: CacheContext, value: CacheValue) -> None:
        self.stores.append((context, value))
