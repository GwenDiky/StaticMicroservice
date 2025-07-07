from fastapi import HTTPException, status
from pymongo.errors import DuplicateKeyError

headers = {"WWW-Authenticate": "Bearer"}


class InvalidIdFormatException(HTTPException):
    def __init__(self) -> None:
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid ID format",
            headers=headers,
        )


class DocumentNotFoundException(HTTPException):
    def __init__(self) -> None:
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Statics not found",
            headers=headers,
        )


class InvalidTaskStatusError(HTTPException):
    def __init__(self) -> None:
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid task status. Choices: in_progress, done, to_do",
            headers=headers,
        )


class ProjectNotFoundError(HTTPException):
    def __init__(self, project_id: int) -> None:
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project with ID {project_id} not found.",
            headers=headers,
        )


class DatabaseUpdateError(HTTPException):
    def __init__(self) -> None:
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error occurred while updating database",
            headers=headers,
        )


class DuplicateProjectError(DuplicateKeyError):
    def __init__(self, project_id: int) -> None:
        super().__init__(f"Project with ID {project_id} already exists.")
