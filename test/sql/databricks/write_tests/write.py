from driver import requires, run_paired


@requires(
    source="${UC_TEST_CATALOG}.source.simple_table",
    access="rw",
    properties={"commit": "plain", "storage": "external"},
)
def test_write_catalog_managed(request, resources):
    run_paired(request, env=resources.env)
