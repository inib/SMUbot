"""Songbot shared bot package marker for cross-service imports.

Description: marks ``bot/`` as a regular Python package so backend and bot
services can import shared helpers (for example ``bot.chat_command_core``)
consistently in container and test environments.
Dependencies: Python import system package discovery.
Code customers: backend and bot runtime modules importing from ``bot.*``.
Used variables/origin: no runtime variables; import resolution derives from
``PYTHONPATH`` and application working directory.
TODO(removal): remove only if shared helpers are relocated to a dedicated
library package.
"""

