"""Generate a large text file of random integers, one per line, to be processed with Spark."""

from pathlib import Path

import numpy as np

DEFAULT_OUTPUT_FILE = "data/output/numbers.txt"
DEFAULT_COUNT = 100_000_000
DEFAULT_SEED = 42
CHUNK_SIZE = 1_000_000  # Integers generated and written at once
MAX_VALUE = 1000  # Integers are drawn from [0, MAX_VALUE)


def generate_numbers(path: str, count: int, seed: int) -> None:
    """Write random integers in [0, MAX_VALUE), one per line, to a file.

    The integers are generated in chunks, so the memory used does not depend on ``count``.

    Args:
        path: Path of the file to create.
        count: Number of integers to write.
        seed: Seed of the random generator.

    Raises:
        ValueError: If ``count`` is negative.
    """
    if count < 0:
        raise ValueError(f"count must be non-negative, got {count}")

    generator = np.random.default_rng(seed)
    with open(path, "w") as output_file:
        for start in range(0, count, CHUNK_SIZE):
            size = min(CHUNK_SIZE, count - start)
            numbers = generator.integers(0, MAX_VALUE, size=size)
            output_file.write("\n".join(map(str, numbers.tolist())) + "\n")


def main(
    output_file: str = DEFAULT_OUTPUT_FILE, count: int = DEFAULT_COUNT, seed: int = DEFAULT_SEED
) -> None:
    """Generate the file of random integers.

    Args:
        output_file: Path of the file to create; its parent directory is created if needed.
        count: Number of integers to write.
        seed: Seed of the random generator.
    """
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    generate_numbers(output_file, count, seed)
    print(f"Integers written to '{output_file}': {count}")


if __name__ == "__main__":
    main()
