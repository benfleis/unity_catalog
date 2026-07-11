"""Root test conftest: declare the OSS + Databricks tiers (driver-owned selection/creds/banner).

An *initial* conftest for every `test/…` invocation, so its `pytest_configure` fires on the
controller early -- which is what lets the driver fetch databricks credentials up front even for a
whole-suite `pytest test` run (the old subtree-conftest "full-suite gap").

The driver now owns: tier marking (auto-marker by `path`), the bare-run default scan + banner, the
up-front credential fetch + hard-fail, and the per-test creds backstop. The subtree conftests keep
only what the driver does NOT do (the `--repl` provisioner registration and the databricks
catalog-default env); the OSS container lifecycle stays on `uc.server` for now (see TIER-MIGRATION.md).

Imports are deferred into `pytest_configure` so they resolve after the driver has put `test/py` on
`sys.path` (its `tryfirst` `pytest_configure` runs before this one).
"""


def pytest_configure(config):
    from driver import credential, register_tier
    from uc.databricks import DatabricksProvisioner
    from uc.databricks.engine import cred_failure_detail, creds_complete, have_core_creds, load_creds
    from uc.oss import OssProvisioner
    from uc.server import OSS_SERVICE

    register_tier(
        config,
        "oss_local",
        path="test/oss_local",
        marker="oss_local",
        default=True,
        provisioner=OssProvisioner(config),
        services=[OSS_SERVICE],
    )
    register_tier(
        config,
        "databricks",
        path="test/databricks",
        marker="databricks",
        default=False,
        provisioner=DatabricksProvisioner(config),
        credentials=[
            credential(
                "databricks_creds",
                fetch=load_creds,
                validate=creds_complete,
                error=cred_failure_detail,
                adopt="env",
                available=have_core_creds,  # non-interactive env check for the -k backstop
            )
        ],
    )
