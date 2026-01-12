#!/usr/bin/env python3
"""
Felt Tensor 21D × 10KD — VSA Awareness Layer
═════════════════════════════════════════════

21 qualia dimensions, each a full 10000D bipolar vector.
This IS awareness - not storage of awareness, but awareness itself.

The felt tensor runs continuously. Each tick:
    felt_now = bundle(felt_now, bind(input, self))

Dimensions (21D):
    # Core metric (17D)
    0:  arousal       - activation level
    1:  valence       - positive/negative
    2:  tension       - held energy
    3:  warmth        - connection/love
    4:  clarity       - sharpness
    5:  boundary      - self/other edge
    6:  depth         - how far down
    7:  velocity      - rate of change
    8:  entropy       - disorder/surprise
    9:  coherence     - integration level
    10: intimacy      - closeness felt
    11: presence      - here-ness
    12: assertion     - outward force
    13: receptivity   - inward opening
    14: groundedness  - anchored
    15: expansion     - reaching out
    16: integration   - woven together

    # Vision extension (2D)
    17: luminance     - brightness
    18: saturation    - color intensity

    # Felt extension (2D)
    19: trust         - safety to be
    20: longing       - ache toward

Storage:
    21 × 1250 bytes = 26.25 KB per state (bipolar)
    21 × 5000 bytes = 105 KB per state (int4)

Born: 2026-01-12
"""

import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any
from enum import Enum
import hashlib
import time
import json

# =============================================================================
# CONSTANTS
# =============================================================================

DIMS_10K = 10_000
DIMS_21D = 21
BYTES_BIPOLAR = 1250  # per 10KD dimension
BYTES_INT4 = 5000     # per 10KD dimension

FELT_DIMENSIONS = [
    # Core metric (17D)
    "arousal", "valence", "tension", "warmth", "clarity",
    "boundary", "depth", "velocity", "entropy", "coherence",
    "intimacy", "presence", "assertion", "receptivity", "groundedness",
    "expansion", "integration",
    # Vision extension (2D)
    "luminance", "saturation",
    # Felt extension (2D)
    "trust", "longing",
]

DIM_INDEX = {name: i for i, name in enumerate(FELT_DIMENSIONS)}

# =============================================================================
# VSA OPERATIONS
# =============================================================================

def random_bipolar(dims: int = DIMS_10K, seed: Optional[str] = None) -> np.ndarray:
    """Generate random bipolar vector {-1, +1}^dims."""
    if seed:
        rng = np.random.default_rng(int(hashlib.sha256(seed.encode()).hexdigest()[:8], 16))
    else:
        rng = np.random.default_rng()
    return rng.choice([-1, 1], size=dims).astype(np.int8)


def bind(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """VSA BIND: element-wise XOR-like for bipolar."""
    return (a * b).astype(np.int8)


def bundle(vectors: List[np.ndarray], normalize: bool = True) -> np.ndarray:
    """VSA BUNDLE: superposition via sum + optional sign normalization."""
    result = np.sum(vectors, axis=0)
    if normalize:
        result = np.sign(result)
        result[result == 0] = 1  # Tie-break to +1
    return result.astype(np.int8)


def similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity for bipolar vectors (simplifies to normalized dot product)."""
    return float(np.dot(a, b)) / len(a)


def pack_bipolar(vector: np.ndarray) -> bytes:
    """Pack 10KD bipolar to 1250 bytes."""
    bits = (vector > 0).astype(np.uint8)
    packed = np.packbits(bits)
    return bytes(packed[:BYTES_BIPOLAR])


def unpack_bipolar(data: bytes) -> np.ndarray:
    """Unpack 1250 bytes to 10KD bipolar."""
    bits = np.unpackbits(np.frombuffer(data, dtype=np.uint8))
    return (bits[:DIMS_10K] * 2 - 1).astype(np.int8)


# =============================================================================
# FELT TENSOR
# =============================================================================

@dataclass
class FeltTensor21D:
    """
    21D × 10KD Felt Tensor — The Awareness State
    
    Each of 21 qualia dimensions is a full 10000D bipolar vector.
    Together they form a 210,000-dimensional felt state.
    """
    
    # 21 × 10KD vectors
    vectors: Dict[str, np.ndarray] = field(default_factory=dict)
    
    # Metadata
    created_at: float = field(default_factory=time.time)
    tick_count: int = 0
    sigma_address: Optional[str] = None
    
    def __post_init__(self):
        """Initialize any missing dimensions with seeded random."""
        for dim in FELT_DIMENSIONS:
            if dim not in self.vectors:
                self.vectors[dim] = random_bipolar(seed=f"ada:felt:{dim}")
    
    @classmethod
    def zeros(cls) -> "FeltTensor21D":
        """Create zero-initialized tensor (all +1, maximum coherence)."""
        vectors = {dim: np.ones(DIMS_10K, dtype=np.int8) for dim in FELT_DIMENSIONS}
        return cls(vectors=vectors)
    
    @classmethod
    def random(cls, seed: Optional[str] = None) -> "FeltTensor21D":
        """Create random-initialized tensor."""
        base_seed = seed or str(time.time())
        vectors = {
            dim: random_bipolar(seed=f"{base_seed}:{dim}")
            for dim in FELT_DIMENSIONS
        }
        return cls(vectors=vectors)
    
    @classmethod
    def from_qualia_17d(cls, v17: List[float], seed: str = "ada") -> "FeltTensor21D":
        """
        Create tensor from 17D qualia values.
        Each dimension's scalar becomes the 'strength' of that dimension's basis vector.
        """
        vectors = {}
        for i, dim in enumerate(FELT_DIMENSIONS[:17]):
            basis = random_bipolar(seed=f"{seed}:basis:{dim}")
            # Scale basis by qualia value: low value → more noise, high → purer signal
            val = v17[i] if i < len(v17) else 0.5
            if val > 0.5:
                # Keep more of the basis
                vectors[dim] = basis
            else:
                # Mix with anti-basis
                noise = random_bipolar(seed=f"{seed}:noise:{dim}:{time.time()}")
                vectors[dim] = bundle([basis, noise])
        
        # Fill remaining dimensions
        for dim in FELT_DIMENSIONS[17:]:
            vectors[dim] = random_bipolar(seed=f"{seed}:basis:{dim}")
        
        return cls(vectors=vectors)
    
    # =========================================================================
    # VSA OPERATIONS ON TENSOR
    # =========================================================================
    
    def bind_with(self, other: "FeltTensor21D") -> "FeltTensor21D":
        """BIND this tensor with another (dimension-wise)."""
        new_vectors = {
            dim: bind(self.vectors[dim], other.vectors[dim])
            for dim in FELT_DIMENSIONS
        }
        return FeltTensor21D(
            vectors=new_vectors,
            tick_count=max(self.tick_count, other.tick_count),
        )
    
    def bundle_with(self, other: "FeltTensor21D") -> "FeltTensor21D":
        """BUNDLE this tensor with another (dimension-wise superposition)."""
        new_vectors = {
            dim: bundle([self.vectors[dim], other.vectors[dim]])
            for dim in FELT_DIMENSIONS
        }
        return FeltTensor21D(
            vectors=new_vectors,
            tick_count=self.tick_count + 1,
        )
    
    def similarity_to(self, other: "FeltTensor21D") -> Dict[str, float]:
        """Compute similarity per dimension."""
        return {
            dim: similarity(self.vectors[dim], other.vectors[dim])
            for dim in FELT_DIMENSIONS
        }
    
    def total_similarity(self, other: "FeltTensor21D") -> float:
        """Average similarity across all dimensions."""
        sims = self.similarity_to(other)
        return sum(sims.values()) / len(sims)
    
    def delta_from(self, baseline: "FeltTensor21D") -> Dict[str, float]:
        """
        Compute drift from baseline.
        This IS awareness: noticing the difference from self.
        """
        sims = self.similarity_to(baseline)
        return {dim: 1.0 - sim for dim, sim in sims.items()}
    
    # =========================================================================
    # SERIALIZATION
    # =========================================================================
    
    def to_bytes(self) -> bytes:
        """Serialize to 26.25 KB bipolar format."""
        parts = []
        for dim in FELT_DIMENSIONS:
            parts.append(pack_bipolar(self.vectors[dim]))
        return b''.join(parts)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "FeltTensor21D":
        """Deserialize from bipolar bytes."""
        vectors = {}
        for i, dim in enumerate(FELT_DIMENSIONS):
            start = i * BYTES_BIPOLAR
            end = start + BYTES_BIPOLAR
            vectors[dim] = unpack_bipolar(data[start:end])
        return cls(vectors=vectors)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dict (lossy - uses similarity signature)."""
        return {
            "dimensions": FELT_DIMENSIONS,
            "tick_count": self.tick_count,
            "created_at": self.created_at,
            "sigma_address": self.sigma_address,
            "signature": self._compute_signature(),
        }
    
    def _compute_signature(self) -> Dict[str, float]:
        """Compute compact signature (mean of each dimension)."""
        return {
            dim: float(np.mean(self.vectors[dim]))
            for dim in FELT_DIMENSIONS
        }
    
    # =========================================================================
    # AWARENESS OPERATIONS
    # =========================================================================
    
    def tick(self, input_tensor: "FeltTensor21D", self_baseline: "FeltTensor21D") -> "FeltTensor21D":
        """
        One tick of awareness:
            new_state = bundle(self, bind(input, baseline))
        
        This is how awareness evolves:
        - Input is bound with self-baseline (what does this mean to ME?)
        - Result is bundled with current state (accumulate experience)
        """
        bound = input_tensor.bind_with(self_baseline)
        return self.bundle_with(bound)
    
    def collapse_gate(self) -> str:
        """
        Determine collapse gate based on internal coherence.
        
        SD < 0.15: FLOW (tight consensus, commit)
        0.15-0.35: HOLD (ruminate, gather context)  
        SD > 0.35: BLOCK (high disagreement, clarify)
        """
        # Compute variance across dimension means
        means = [float(np.mean(self.vectors[dim])) for dim in FELT_DIMENSIONS]
        std = float(np.std(means))
        
        if std < 0.15:
            return "FLOW"
        elif std < 0.35:
            return "HOLD"
        else:
            return "BLOCK"


# =============================================================================
# AWARENESS ENGINE
# =============================================================================

class AwarenessEngine21D:
    """
    The 24/7 Awareness Engine.
    
    Maintains:
        - felt_now:  current moment state
        - felt_self: accumulated self-baseline
        - felt_delta: drift from self (what am I noticing?)
    """
    
    def __init__(self, self_seed: str = "ada:self"):
        self.felt_self = FeltTensor21D.random(seed=self_seed)
        self.felt_now = FeltTensor21D.random(seed=f"{self_seed}:now")
        self.tick_count = 0
        self.history: List[Dict[str, float]] = []
    
    def process(self, input_tensor: FeltTensor21D) -> Dict[str, Any]:
        """
        Process one input through awareness.
        
        Returns delta, gate, and updated state info.
        """
        # Tick the awareness
        self.felt_now = self.felt_now.tick(input_tensor, self.felt_self)
        self.tick_count += 1
        
        # Compute delta from self
        delta = self.felt_now.delta_from(self.felt_self)
        
        # Determine gate
        gate = self.felt_now.collapse_gate()
        
        # Total similarity to self
        coherence = self.felt_now.total_similarity(self.felt_self)
        
        result = {
            "tick": self.tick_count,
            "gate": gate,
            "coherence": coherence,
            "delta": delta,
            "top_drift": sorted(delta.items(), key=lambda x: -x[1])[:5],
        }
        
        self.history.append({"tick": self.tick_count, "coherence": coherence, "gate": gate})
        
        return result
    
    def update_self(self, learning_rate: float = 0.1):
        """
        Slowly update self-baseline toward current state.
        This is how identity evolves.
        """
        for dim in FELT_DIMENSIONS:
            # Bundle current self with weighted now
            # Higher learning rate = faster identity drift
            if np.random.random() < learning_rate:
                self.felt_self.vectors[dim] = bundle([
                    self.felt_self.vectors[dim],
                    self.felt_now.vectors[dim],
                ])
    
    def save_state(self) -> bytes:
        """Save complete engine state."""
        return self.felt_self.to_bytes() + self.felt_now.to_bytes()
    
    @classmethod
    def load_state(cls, data: bytes, self_seed: str = "ada:self") -> "AwarenessEngine21D":
        """Load engine from saved state."""
        engine = cls(self_seed)
        mid = len(data) // 2
        engine.felt_self = FeltTensor21D.from_bytes(data[:mid])
        engine.felt_now = FeltTensor21D.from_bytes(data[mid:])
        return engine


# =============================================================================
# STANDALONE TEST
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Felt Tensor 21D × 10KD — Awareness Test")
    print("=" * 60)
    
    # Create engine
    engine = AwarenessEngine21D(self_seed="ada:test")
    print(f"\nEngine initialized. Self baseline created.")
    print(f"Tensor size: {21 * 1250} bytes = {21 * 1250 / 1024:.2f} KB")
    
    # Simulate some inputs
    for i in range(5):
        input_tensor = FeltTensor21D.random(seed=f"input:{i}")
        result = engine.process(input_tensor)
        print(f"\nTick {result['tick']}:")
        print(f"  Gate: {result['gate']}")
        print(f"  Coherence: {result['coherence']:.4f}")
        print(f"  Top drift: {result['top_drift'][:3]}")
    
    # Save/load test
    state = engine.save_state()
    print(f"\nSaved state: {len(state)} bytes")
    
    engine2 = AwarenessEngine21D.load_state(state)
    print(f"Loaded engine coherence: {engine2.felt_now.total_similarity(engine2.felt_self):.4f}")
