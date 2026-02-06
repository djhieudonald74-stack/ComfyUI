"""
Add is_missing column to asset_cache_state for non-destructive soft-delete

Revision ID: 0002_add_is_missing
Revises: 0001_assets
Create Date: 2025-02-05 00:00:00
"""

from alembic import op
import sqlalchemy as sa

revision = "0002_add_is_missing"
down_revision = "0001_assets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "asset_cache_state",
        sa.Column(
            "is_missing",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.create_index(
        "ix_asset_cache_state_is_missing",
        "asset_cache_state",
        ["is_missing"],
    )


def downgrade() -> None:
    op.drop_index("ix_asset_cache_state_is_missing", table_name="asset_cache_state")
    op.drop_column("asset_cache_state", "is_missing")
