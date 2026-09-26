import secrets
from typing import Optional
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from vault.config import VAULT_ADMIN_USER, VAULT_ADMIN_PASSWORD

security_basic = HTTPBasic(auto_error=False)

def verify_admin_auth(credentials: Optional[HTTPBasicCredentials] = Depends(security_basic)) -> str:
    """
    Timing-attack safe verification of administrative credentials using HTTP Basic Auth.
    Enforces access control for /admin and destructive chaos simulation endpoints.
    """
    unauthorized_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid administrative credentials",
        headers={"WWW-Authenticate": 'Basic realm="Vault Admin Console"'}
    )

    if not credentials:
        raise unauthorized_exc

    correct_username = secrets.compare_digest(
        credentials.username.encode("utf-8"),
        VAULT_ADMIN_USER.encode("utf-8")
    )
    correct_password = secrets.compare_digest(
        credentials.password.encode("utf-8"),
        VAULT_ADMIN_PASSWORD.encode("utf-8")
    )

    if not (correct_username and correct_password):
        raise unauthorized_exc

    return credentials.username
