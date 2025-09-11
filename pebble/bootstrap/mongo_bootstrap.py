from ..mongo_client import ensure_indexes
from ..config_loader import Settings


def bootstrap_mongo(settings: Settings):
    ensure_indexes(settings)
    return {"ok": True}
