from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


class Database:
    def __init__(self, url: str) -> None:
        self._engine: AsyncEngine = create_async_engine(url, pool_pre_ping=True)
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self._engine,
            expire_on_commit=False,
        )

    def get_engine(self) -> AsyncEngine:
        return self._engine

    def get_session(self) -> AsyncSession:
        return self._session_factory()

    def get_session_factory(self) -> async_sessionmaker[AsyncSession]:
        return self._session_factory
