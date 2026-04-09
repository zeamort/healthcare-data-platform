"""
Generic ML Lambda handler.

The ML_MODULE environment variable selects which ML script to run:
  ml_redshift or ml_comorbidity.

Supports an optional 'action' key in the event payload for modules
that accept it (e.g. ml_redshift: 'create' or 'apply').
"""

import os
import importlib
import inspect
import json
import logging

log = logging.getLogger()
log.setLevel(logging.INFO)

ML_MODULE = os.environ.get("ML_MODULE", "ml_redshift")


def lambda_handler(event, context):
    """Import and run the configured ML module's main()."""
    log.info("Running ML module: %s (event: %s)", ML_MODULE,
             json.dumps(event, default=str))

    mod = importlib.import_module(ML_MODULE)

    # Pass 'action' to main() if the module supports it
    action = event.get("action") if isinstance(event, dict) else None
    sig = inspect.signature(mod.main)
    if sig.parameters:
        mod.main(action=action)
    else:
        mod.main()

    log.info("%s complete.", ML_MODULE)
    return {"status": "complete", "module": ML_MODULE}
