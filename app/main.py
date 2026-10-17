from fastapi import FastAPI


def create_app() -> FastAPI:
    application: FastAPI = FastAPI(title="Async Processing Service", docs_url=None, redoc_url=None,
                                   openapi_url=None)
    return application
