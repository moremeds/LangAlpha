"""Free the ``futu`` name for the connector shipped with it.

The pass ``037_free_webull_name`` did for ``webull``, done here for ``futu``,
for the reasons 030 and 037 give and do not need repeating: a brokerage's name
is its identity, and the write paths that refuse the name cannot repair a row
already sitting on it. Futu's endpoint is moomoo's server on Futu's own host,
so a user with a Futu account may well have added it by hand already.

Revision ID: 090
Revises: 089
"""

from alembic import op


revision = "090"
down_revision = "089"
branch_labels = None
depends_on = None

# Pinned rather than imported, for the reason 030 gives: BROKERAGES is free to
# grow after this runs, and a later entry is that entry's migration to write.
#
# The address exemption is 030's, asked as 032's host test rather than 030's
# textual prefix because that is the question ``brokerage_for_url`` answers and
# the two have to agree: whatever this leaves under the name is drawn as that
# vendor by a page that joined on it. Parsing lowercases the host and drops the
# port, so ``HTTPS://MCP.FUTUNN.COM:443/mcp`` is the connector, while a prefix
# reads it as somebody else's server and renames it out from under a live
# connection. The trailing anchor keeps it a host test rather than a longer
# prefix, so ``mcp.futunn.com.evil.com`` is still somebody else.
_SHIPPED = r"""(VALUES
    ('futu', '^https?://mcp\.futunn\.com(:[0-9]+)?([/?#]|$)')
) AS shipped(name, vendor_host)"""
_RESERVED = "('futu')"


def upgrade() -> None:
    # --- user tier -------------------------------------------------------
    # The scratch tables carry this revision in their names because ON COMMIT
    # DROP means "at the end of the transaction", and alembic runs the whole
    # upgrade in ONE transaction. A database coming from 029 or earlier still
    # holds 030's and 032's identically-shaped tables when this runs, so a bare
    # name aborts every fresh install with "relation already exists".
    #
    # The suffix is checked free against all three tables this rename writes a
    # name into, each of which has its own UNIQUE over it. The OAuth connection
    # is the one that can be holding a name nothing else holds, so it gets its
    # own check rather than riding on the server row's.
    op.execute(f"""
        CREATE TEMP TABLE mcp_user_renames_090 ON COMMIT DROP AS
        SELECT u.user_id,
               u.name AS old_name,
               CASE WHEN EXISTS (
                        SELECT 1 FROM user_mcp_servers x
                         WHERE x.user_id = u.user_id
                           AND x.name = u.name || '_legacy'
                    ) OR EXISTS (
                        SELECT 1
                          FROM workspace_mcp_servers w
                          JOIN workspaces ws USING (workspace_id)
                         WHERE ws.user_id = u.user_id
                           AND w.name = u.name || '_legacy'
                    ) OR EXISTS (
                        SELECT 1 FROM user_mcp_oauth_connections c
                         WHERE c.user_id = u.user_id
                           AND c.server_name = u.name || '_legacy'
                    )
                    THEN u.name || '_legacy_'
                         || replace(u.user_mcp_server_id::text, '-', '')
                    ELSE u.name || '_legacy'
               END AS new_name
          FROM user_mcp_servers u
          JOIN {_SHIPPED} ON shipped.name = u.name
         WHERE u.plugin_id IS NOT NULL
            OR u.url IS NULL
            OR u.url !~* shipped.vendor_host
    """)

    op.execute("""
        UPDATE user_mcp_servers u SET name = r.new_name
          FROM mcp_user_renames_090 r
         WHERE u.user_id = r.user_id AND u.name = r.old_name
    """)
    # Moves with the row or the freed name inherits a live grant and the page
    # draws somebody else's server as a connected broker.
    op.execute("""
        UPDATE user_mcp_oauth_connections c SET server_name = r.new_name
          FROM mcp_user_renames_090 r
         WHERE c.user_id = r.user_id AND c.server_name = r.old_name
    """)
    op.execute("""
        UPDATE user_mcp_tool_schemas s SET server_name = r.new_name
          FROM mcp_user_renames_090 r
         WHERE s.user_id = r.user_id AND s.server_name = r.old_name
    """)
    # source='user' rows are tombstones naming an inherited user row that is
    # switched off in this workspace. Left behind, one would switch off the
    # brokerage the user connects later, in a workspace they never chose it for.
    op.execute("""
        UPDATE workspace_mcp_servers w SET name = r.new_name
          FROM mcp_user_renames_090 r, workspaces ws
         WHERE w.workspace_id = ws.workspace_id
           AND ws.user_id = r.user_id
           AND w.source = 'user'
           AND w.name = r.old_name
    """)

    # --- consent for the rows this pass just vouched for ------------------
    # As 037's: the rows still holding the name now are the connector's, and a
    # NULL record would refuse every curated tool until a reconnect. moomoo's
    # non-danger set from 036's ``_BACKFILL``, since Futu shares its groups.
    op.execute("""
        UPDATE user_mcp_oauth_connections
           SET granted_capabilities =
               '["market_data","watchlists","account","paper_trading"]'::jsonb
         WHERE server_name = 'futu'
           AND granted_capabilities IS NULL
    """)

    # --- workspace tier --------------------------------------------------
    # No address test: a brokerage connector only ever exists at the user tier,
    # so a workspace-local row under the name is always somebody's own server,
    # and always the one the resolver has already stopped running.
    op.execute(f"""
        CREATE TEMP TABLE mcp_workspace_renames_090 ON COMMIT DROP AS
        SELECT w.workspace_mcp_server_id,
               w.workspace_id,
               w.name AS old_name,
               CASE WHEN EXISTS (
                        SELECT 1 FROM workspace_mcp_servers x
                         WHERE x.workspace_id = w.workspace_id
                           AND x.name = w.name || '_local'
                    )
                    THEN w.name || '_local_'
                         || replace(w.workspace_mcp_server_id::text, '-', '')
                    ELSE w.name || '_local'
               END AS new_name
          FROM workspace_mcp_servers w
         WHERE w.source = 'workspace' AND w.name IN {_RESERVED}
    """)

    # The name on the row wins over any name inside the config blob
    # (workspace_row_to_server_config), so the blob is left as it is.
    op.execute("""
        UPDATE workspace_mcp_servers w SET name = r.new_name
          FROM mcp_workspace_renames_090 r
         WHERE w.workspace_mcp_server_id = r.workspace_mcp_server_id
    """)
    op.execute("""
        UPDATE workspace_mcp_tool_schemas s SET server_name = r.new_name
          FROM mcp_workspace_renames_090 r
         WHERE s.workspace_id = r.workspace_id AND s.server_name = r.old_name
    """)

    # Every workspace whose effective set just changed, so a session holding the
    # old version re-resolves instead of running the renamed row under its old
    # name until something else happens to bump it.
    op.execute("""
        UPDATE workspaces ws SET mcp_config_version = ws.mcp_config_version + 1
         WHERE ws.workspace_id IN (SELECT workspace_id FROM mcp_workspace_renames_090)
            OR ws.user_id IN (SELECT user_id FROM mcp_user_renames_090)
    """)


def downgrade() -> None:
    # Deliberately empty, as 030's and 032's are. A renamed row is
    # indistinguishable from one a user named that way, and restoring the
    # reserved name would re-create the collision this exists to clear, on a
    # schema that still refuses it.
    pass
