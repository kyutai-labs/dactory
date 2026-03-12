from .dactory import *  # noqa: F403

__doc__ = dactory.__doc__  # noqa: F405
if hasattr(dactory, "__all__"):  # noqa: F405
    __all__ = dactory.__all__  # noqa: F405

from beartype import BeartypeConf
from beartype.claw import beartype_this_package

beartype_this_package(conf=BeartypeConf(is_color=False))
