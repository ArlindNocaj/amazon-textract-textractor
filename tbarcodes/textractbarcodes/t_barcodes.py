import logging
import trp.trp2 as t2
import os
from typing import List, Union
from dataclasses import dataclass, asdict
from PIL import Image
from PyPDF2 import PdfFileReader
import boto3
import io
from docbarcodes.extract import process_document
logger = logging.getLogger(__name__)
from trp.trp2 import TBlock, TGeometry, TBoundingBox, TPoint
from uuid import uuid4, UUID

from loguru import logger
import sys
logger.remove()
logger.add(sys.stderr, level="ERROR")

pdf_suffixes = ['.pdf']
image_suffixes = ['.png', '.jpg', '.jpeg']
supported_suffixes = pdf_suffixes + image_suffixes

def add_page_barcodes(t_document: t2.TDocument,
                      input_document: Union[str, bytes]) -> t2.TDocument:
    """
    extracts and adds barcode data to each page of the document in the form of key-value pairs
    """
    # page_dimensions: List[DocumentDimensions] = list()

    if len(input_document) > 7 and input_document.lower().startswith(
            "s3://"):
        with tempfile.TemporaryDirectory() as d:
            target = os.path.join(d, objectName)
            os.makedirs(Path(target).parent, exist_ok=True)
            s3.download_file(bucketName, objectName, target)
            barcodes_raw, barcodes_combined = process_document(target, max_pages=None, use_jpype=True)

    else:
        barcodes_raw, barcodes_combined = process_document(input_document, max_pages=None, use_jpype=True)

    if isinstance(input_document, str):
        pass

    # TODO do we need bytes and bytearray support, currently the lib assumes we have a file
    # elif isinstance(input_document, (bytes, bytearray)):
    #     page_dimensions = get_size_from_filestream(io.BytesIO(input_document),
    #                                                ext=None)
    for b in barcodes_raw:
        id = str(uuid4())
        value_block = TBlock(
            id=id,
            block_type="KEY_VALUE_SET",
            entity_types=["VALUE"],
            confidence=99,
            geometry=TGeometry(bounding_box=TBoundingBox(width=0, height=0, left=0, top=0),
                               polygon=[TPoint(x=0, y=0), TPoint(x=0, y=0)]),
            text = b.raw,
            page = b.page,
            custom = {"resultMetadata": b.resultMetadata}
        )
        t_document.add_key_values(b.format, [value_block], None)

    for b in barcodes_combined:
        id = str(uuid4())
        value_block = TBlock(
            id=id,
            block_type="KEY_VALUE_SET",
            entity_types=["VALUE"],
            confidence=99,
            geometry=TGeometry(bounding_box=TBoundingBox(width=0, height=0, left=0, top=0),
                               polygon=[TPoint(x=0, y=0), TPoint(x=0, y=0)]),
            text = b.content,
            page = barcodes_raw[b.sources[0]].page,
            custom = {"sources": b.sources}
        )
        t_document.add_key_values(b.format, [value_block], None)
    #t_document.add_key_values()
    # bytes do not return a page for the Block, cannot use the mapping logic as above
    return t_document
