"""Conservative Firestore document/batch budgets for public publication."""

from google.cloud.firestore_v1._helpers import encode_dict
from google.cloud.firestore_v1.types import Document

# Leave headroom for paths, server timestamps and index/storage overhead.
MAX_DOCUMENT_BYTES = 900_000
MAX_BATCH_BYTES = 8_000_000
MAX_BATCH_WRITES = 400


def document_bytes(data: dict) -> int:
    """Measure the actual encoded document, rather than JSON character count."""
    return Document(fields=encode_dict(data))._pb.ByteSize() + 1024


def publication_batches(writes):
    """Preflight every payload before staging any writes, then bound each batch."""
    batches, batch, size = [], [], 0
    for ref, data in writes:
        encoded = document_bytes(data)
        if encoded > MAX_DOCUMENT_BYTES:
            raise ValueError(
                "A result document exceeds the supported Firestore size budget"
            )
        if batch and (
            len(batch) >= MAX_BATCH_WRITES or size + encoded > MAX_BATCH_BYTES
        ):
            batches.append(batch)
            batch, size = [], 0
        batch.append((ref, data))
        size += encoded
    if batch:
        batches.append(batch)
    return batches
