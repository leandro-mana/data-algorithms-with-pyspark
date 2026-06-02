# Bonus Chapter: K-mers

This chapter extends Chapter 2's DNA analysis work from base counts to **k-mer counting** — finding the most frequent substrings of length K across a set of DNA reads. K-mers are the foundation of genome assembly, species identification from metagenomic samples, and variant analysis.

## Examples

| Example | Description | Key Concepts |
| --- | --- | --- |
| `01_kmer_fasta.py` | Count k-mers from FASTA sequences — two approaches compared | flatMap sliding window, mapPartitions in-mapper combiner, broadcast K, Top-N |
| `02_kmer_fastq.py` | Count k-mers from FASTQ reads with Phred quality filtering | zipWithIndex, join by record index, Phred+33 decoding, quality threshold |

## Running Examples

```bash
# FASTA k-mer counting (K=4, Top-10)
make run-spark CHAPTER=bonus_chapters/k_mers EXAMPLE=01_kmer_fasta

# Custom K and Top-N
make run-spark CHAPTER=bonus_chapters/k_mers EXAMPLE=01_kmer_fasta \
    ARGS="src/bonus_chapters/k_mers/data/sample.fasta 5 5"

# FASTQ k-mer counting with quality filter (K=4, Top-10, Min Q=20)
make run-spark CHAPTER=bonus_chapters/k_mers EXAMPLE=02_kmer_fastq

# Custom quality threshold
make run-spark CHAPTER=bonus_chapters/k_mers EXAMPLE=02_kmer_fastq \
    ARGS="src/bonus_chapters/k_mers/data/sample.fastq 4 10 30"
```

## Key Concepts

### What Are K-mers?

A k-mer is every contiguous substring of length K in a DNA sequence. For the sequence `AGAT`:

| K | K-mers | Count |
| --- | --- | --- |
| 1 (monomers) | `A`, `G`, `A`, `T` | 4 |
| 2 | `AG`, `GA`, `AT` | 3 |
| 3 | `AGA`, `GAT` | 2 |
| 4 | `AGAT` | 1 |

A sequence of length L produces **L - K + 1** k-mers. Common values of K in genomics: 21, 31, 51 (odd numbers avoid palindromes). In this chapter we use K=4 to keep examples readable.

### FASTA vs FASTQ Formats

Both are text-based formats for DNA sequences from sequencers:

| Feature | FASTA | FASTQ |
| --- | --- | --- |
| Header | `>name description` | `@name description` |
| Sequence | Following lines | Line 2 of 4-line record |
| Quality | None | Line 4: Phred+33 ASCII scores |
| Use case | Reference genomes, assembled sequences | Raw sequencer reads |

```
# FASTA
>sequence_1 description
ATCGATCGATCGATCGATCG
GCTAGCTAGCTAGCTAGCTA

# FASTQ (4 lines per read)
@read_1 description
ATCGATCGATCGATCGATCG
+
IIIIIIIIIIIIIIIIIIII
```

### Sliding Window K-mer Generation

The core transformation: for each sequence, emit one `(kmer, 1)` pair per position:

```python
def generate_kmers(sequence: str, k: int) -> list[tuple[str, int]]:
    seq = sequence.strip().upper()
    if len(seq) < k:
        return []
    return [(seq[i : i + k], 1) for i in range(len(seq) - k + 1)]

# "ATCG" with k=3 → [("ATC", 1), ("TCG", 1)]
```

### Two Approaches to K-mer Counting

Mirrors the three-version pattern from Chapter 2:

#### Approach 1: flatMap per sequence

```python
sequences.flatMap(lambda seq: generate_kmers(seq, k)).reduceByKey(lambda a, b: a + b)
```

For a file with N total bases across P partitions: emits **O(N)** intermediate pairs.

#### Approach 2: mapPartitions (in-mapper combiner)

```python
sequences.mapPartitions(kmers_per_partition(k)).reduceByKey(lambda a, b: a + b)
```

Each partition builds a local dict and emits at most **4^K** pairs (one per unique k-mer found).
For K=4, that's at most 256 pairs per partition vs potentially millions with flatMap.

```python
def kmers_per_partition(k):
    def _aggregate(sequences):
        local = defaultdict(int)
        for seq in sequences:
            stripped = seq.strip().upper()
            for i in range(len(stripped) - k + 1):
                local[stripped[i : i + k]] += 1
        return local.items()
    return _aggregate
```

### FASTQ Parsing with zipWithIndex

FASTQ records span 4 lines. `textFile` gives individual lines — we use `zipWithIndex` to recover structure:

```python
indexed = sc.textFile(fastq_path).zipWithIndex()

# Position within record = global_index % 4
# Record identifier     = global_index // 4  (used as join key)
sequences = indexed.filter(lambda li: li[1] % 4 == 1).map(lambda li: (li[1] // 4, li[0]))
qualities  = indexed.filter(lambda li: li[1] % 4 == 3).map(lambda li: (li[1] // 4, li[0]))

# Join on record index → pair (sequence, quality) for each read
pairs = sequences.join(qualities).values()
```

### Phred Quality Scores

Sequencers report confidence per base as a Phred score:

```
Q = -10 × log₁₀(P)    where P = probability of wrong base call

Q10  →  10% error rate   (1 in 10 wrong)
Q20  →   1% error rate   (1 in 100 wrong)  ← common filter threshold
Q30  →  0.1% error rate  (1 in 1000 wrong)
Q40  →  0.01% error rate (high-end sequencers)
```

ASCII encoding (Phred+33): `score = ord(char) - 33`

| Char | ord | Phred score | Quality |
| --- | --- | --- | --- |
| `!` | 33 | 0 | Unusable |
| `5` | 53 | 20 | Acceptable (Q20) |
| `I` | 73 | 40 | Excellent |

```python
def meets_quality(sequence: str, quality: str, min_q: int) -> bool:
    return len(sequence) == len(quality) and all(ord(c) - 33 >= min_q for c in quality)
```

## Connection to Other Chapters

| Concept | This chapter | Original chapter |
| --- | --- | --- |
| FASTA format | `01_kmer_fasta.py` | Chapter 2 — DNA Base Count |
| mapPartitions in-mapper combiner | `01_kmer_fasta.py` (Approach 2) | Chapter 3 — mapPartitions |
| Top-N pattern | both examples | Chapter 10 — Top-N with heaps |
| Reduce-side join | `02_kmer_fastq.py` (seq + quality) | Chapter 11 — Join Design Patterns |

## Performance Considerations

| Approach | Intermediate pairs | Shuffle volume | Recommended when |
| --- | --- | --- | --- |
| flatMap | O(N) — one per kmer position | High | Small datasets, quick exploration |
| mapPartitions | O(P × 4^K) — at most 4^K per partition | Low | Large datasets (production default) |

For K=4 across 1M reads of length 150 bp:
- flatMap emits ~147M pairs
- mapPartitions emits at most `partitions × 256` pairs — orders of magnitude less

**K choice trade-offs:**

| K | Specificity | Memory | Collision probability |
| --- | --- | --- | --- |
| Small (4–8) | Low — many sequences share k-mers | Low | High |
| Medium (21–31) | High — unique in most genomes | Moderate | Low |
| Large (51+) | Very high | High | Very low |

## Additional Resources

- [K-mer — Wikipedia](https://en.wikipedia.org/wiki/K-mer)
- [FASTQ Format — Wikipedia](https://en.wikipedia.org/wiki/FASTQ_format)
- [Phred Quality Score — Wikipedia](https://en.wikipedia.org/wiki/Phred_quality_score)
- [K-mer counting, part I (Bernardo J. Clavijo)](https://bioinfologics.github.io/post/2018/09/17/k-mer-counting-part-i-introduction/)
