"""Store the destination exchange with every outbox event."""

from alembic import op
import sqlalchemy as sa

revision = "0003_outbox_exchange_name"
down_revision = "0002_link_crawl_integrity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "outbox_events",
        sa.Column("exchange_name", sa.String(length=128), server_default="crawler.topic", nullable=False),
    )
    op.alter_column("outbox_events", "exchange_name", server_default=None)


def downgrade() -> None:
    op.drop_column("outbox_events", "exchange_name")
