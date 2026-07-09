"""Driver for write.test -- seed the id_name fixture into a plain/external cell and write.

@requires(access="rw") seeds `id_name` into an isolated `plain__external__<token>` cell (the
provisioner create+inserts the fixture, injects CATALOG/SCHEMA, drops the cell on
teardown). The body round-trips an INSERT against it.
"""

from driver import Fixture, requires, run_paired


@requires(source=Fixture("id_name"), access="rw", properties={"commit": "plain", "storage": "external"})
def test_write(request, resources):
    run_paired(request, env=resources.env)
