"""Keep unmarked Hermes progress out of Ando's durable reply path."""

from .protocol import SetupRequired

REPLY_ONLY_SETTINGS = {
    "tool_progress": "off",
    "interim_assistant_messages": False,
}


def configure_reply_only(config):
    # Explicit platform values win over global settings; a display tier does not.
    platform = (
        config.setdefault("display", {}).setdefault("platforms", {}).setdefault("ando", {})
    )
    platform.update(REPLY_ONLY_SETTINGS)


def check_reply_only(config):
    try:
        from gateway.display_config import resolve_display_setting
    except ImportError as exc:
        raise SetupRequired(
            "Update Hermes to support per-platform display settings"
        ) from exc

    for setting, expected in REPLY_ONLY_SETTINGS.items():
        actual = resolve_display_setting(config, "ando", setting)
        if actual != expected:
            raise SetupRequired(
                f"Ando requires display.platforms.ando.{setting}={expected!r}; "
                "stop the gateway and set display.platforms.ando.tool_progress: \"off\" "
                "and display.platforms.ando.interim_assistant_messages: false in config.yaml, "
                "then restart it. Invitation profiles can instead run `hermes ando connect`. "
                "Tool progress must not be sent as a reply."
            )
