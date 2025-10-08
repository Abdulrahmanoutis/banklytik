import json
from collections import Counter

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from statements.models import BankStatement
from statements.views import get_s3_client, s3_key_exists


class Command(BaseCommand):
    help = "Inspect cached Textract JSON for a BankStatement"

    def add_arguments(self, parser):
        parser.add_argument("statement_id", type=int, help="ID of the BankStatement to inspect")
        parser.add_argument(
            "--max-cells",
            type=int,
            default=20,
            help="Maximum number of sample CELL texts to print",
        )

    def handle(self, *args, **options):
        statement_id = options["statement_id"]
        max_cells = options["max_cells"]

        try:
            stmt = BankStatement.objects.get(pk=statement_id)
        except BankStatement.DoesNotExist:
            raise CommandError(f"BankStatement with ID {statement_id} does not exist")

        s3 = get_s3_client()
        json_key = f"{stmt.title}.json"

        if not s3_key_exists(settings.AWS_S3_BUCKET, json_key):
            raise CommandError(f"No cached Textract JSON found for {json_key}")

        obj = s3.get_object(Bucket=settings.AWS_S3_BUCKET, Key=json_key)
        blocks_data = json.loads(obj["Body"].read().decode("utf-8"))
        blocks = blocks_data.get("Blocks", blocks_data)

        if not isinstance(blocks, list):
            raise CommandError("Cached JSON does not contain a valid 'Blocks' list")

        counts = Counter(b.get("BlockType") for b in blocks)
        self.stdout.write(self.style.SUCCESS(f"✅ Loaded Textract JSON for statement {statement_id}"))
        self.stdout.write(f"Total blocks: {len(blocks)}")
        for k, v in counts.items():
            self.stdout.write(f"  {k}: {v}")

        # Print sample CELLs
        cell_blocks = [b for b in blocks if b.get("BlockType") == "CELL"]
        self.stdout.write(f"\nFirst {min(max_cells, len(cell_blocks))} CELL blocks:")
        for c in cell_blocks[:max_cells]:
            row = c.get("RowIndex")
            col = c.get("ColumnIndex")
            text = ""
            for rel in c.get("Relationships", []) or []:
                if rel.get("Type") == "CHILD":
                    for wid in rel.get("Ids", []):
                        w = next((b for b in blocks if b["Id"] == wid), None)
                        if not w:
                            continue
                        if w.get("BlockType") == "WORD":
                            text += w.get("Text", "") + " "
                        elif w.get("BlockType") == "SELECTION_ELEMENT":
                            if w.get("SelectionStatus") == "SELECTED":
                                text += "[X] "
            text = text.strip()
            self.stdout.write(f"  Row {row}, Col {col}: {text}")

        # If no tables, print some LINE blocks
        if counts.get("TABLE", 0) == 0:
            line_blocks = [b for b in blocks if b.get("BlockType") == "LINE"]
            self.stdout.write(f"\nFirst {min(10, len(line_blocks))} LINE blocks:")
            for l in line_blocks[:10]:
                self.stdout.write(f"  Page {l.get('Page')}: {l.get('Text')}")
