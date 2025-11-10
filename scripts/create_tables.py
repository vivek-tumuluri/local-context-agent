from app.db import engine

from app.models import (
    Base,
    IngestionJob,
    SourceState,
    ContentIndex,
    DriveSession,
    User,
    UserSession,
)

if __name__ == "__main__":

    Base.metadata.create_all(bind=engine)


    created_tables = ", ".join(sorted(Base.metadata.tables.keys()))
    print(f"Tables created: {created_tables}")
