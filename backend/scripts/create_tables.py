from app.core.db import engine
from app.core.db_utils import ensure_vector_extension, create_doc_chunk_indexes

from app.core.models import (
    Base,
    IngestionJob,
    SourceState,
    ContentIndex,
    DriveSession,
    User,
    UserSession,
)

if __name__ == "__main__":

    ensure_vector_extension(engine)
    Base.metadata.create_all(bind=engine)
    create_doc_chunk_indexes(engine)


    created_tables = ", ".join(sorted(Base.metadata.tables.keys()))
    print(f"Tables created: {created_tables}")
