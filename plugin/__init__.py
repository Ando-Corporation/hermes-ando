"""Ando platform plugin. Keep discovery free of transport/SDK side effects."""


def register(ctx):
    from .adapter import register as register_adapter

    register_adapter(ctx)
    from .cli import setup_parser, run

    ctx.register_cli_command(
        name="ando",
        help="Connect this Hermes profile using an existing Ando agent invitation",
        setup_fn=setup_parser,
        handler_fn=run,
    )
