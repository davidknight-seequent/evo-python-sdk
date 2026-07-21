"""Authentication helpers around the Evo Python SDK.

The heavy lifting (OAuth2 + PKCE against Bentley IMS, discovery, workspace selection) is done
by ``evo.notebooks.ServiceManagerWidget``. This module just wraps it so the rest of the widget
code has a single, stable place to obtain an authenticated ``connector`` and an ``environment``.

The native app performs the exact same flow:
  * PKCE OAuth2 against ``https://ims.bentley.com/connect/authorize``
  * discovery against ``https://discover.api.seequent.com/evo/identity/v2/discovery``
  * workspace + object browsing against the org's hub URL

In Python you get all of that for free from the SDK's ``ServiceManagerWidget``.
"""

from __future__ import annotations

from typing import Any

try:
    # Provided by the Evo Python SDK (evo-sdk-common).
    from evo.notebooks import ServiceManagerWidget
except ImportError as exc:  # pragma: no cover - environment specific
    raise ImportError(
        "The Evo Python SDK is required. Install it with `pip install evo-sdk-common` "
        "(the package that provides `evo.notebooks.ServiceManagerWidget`)."
    ) from exc


async def login(
    *,
    client_id: str,
    redirect_url: str,
    **kwargs: Any,
) -> ServiceManagerWidget:
    """Run the interactive Evo login flow and return a logged-in ``ServiceManagerWidget``.

    Parameters mirror the SDK. ``client_id`` and ``redirect_url`` come from your registered
    Evo application. Any extra keyword arguments are forwarded to
    ``ServiceManagerWidget.with_auth_code``.

    This must be awaited from an async notebook cell::

        manager = await login(client_id="...", redirect_url="http://localhost:3000/signin")
    """
    manager = await ServiceManagerWidget.with_auth_code(
        client_id=client_id,
        redirect_url=redirect_url,
        **kwargs,
    ).login()
    return manager


def get_connector(manager: ServiceManagerWidget):
    """Return the authenticated API connector used to call Evo services."""
    return manager.get_connector()


def get_environment(manager: ServiceManagerWidget):
    """Return the selected environment (``hub_url``, ``org_id``, ``workspace_id``)."""
    return manager.get_environment()
