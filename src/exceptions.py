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


