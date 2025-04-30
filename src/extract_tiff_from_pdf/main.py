#!/usr/bin/env python3

import os
import sys
import argparse
from pypdf import PdfReader
from pdf2image import convert_from_bytes
import concurrent.futures

def extract_tiff_from_pdf(
    pdf_path: str,
    output_directory: str,
    resolution: int = 300
) -> None:
    """
    Extract all pages of a PDF as TIFF images with multi-threading support.
    
    This function serves as the main public interface for the PDF to TIFF conversion
    functionality. It can be used both programmatically and through the command line.
    
    Args:
        pdf_input: Path to the PDF file (str)
        output_directory: Directory where TIFF images will be saved
        resolution: The DPI resolution for the output images (default: 300)
    
    Raises:
        ValueError: If resolution is not positive or if pdf_input is invalid
        OSError: If there are issues with file operations
        Exception: For other processing errors
    """
    # Input validation
    if resolution <= 0:
        raise ValueError("Resolution must be a positive integer")
    
    # Check to make sure the path exists
    if not os.path.isfile(pdf_path):
        raise ValueError(f"PDF file not found: {pdf_path}")

    with open(pdf_path, 'rb') as pdf_file:
        pdf_reader = PdfReader(pdf_file)
        total_pages = len(pdf_reader.pages)
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    # Open the PDF for reading and load it in memory
    raw_pdf_bytes = open(pdf_path, 'rb').read()

    # Create a thread pool executor with up to 20 workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # Convert each page to TIFF
        futures = []
        for page_number in range(total_pages):
            arguments = (raw_pdf_bytes, resolution, output_directory, page_number)
            futures.append(
                executor.submit(_convert_page_to_image, *arguments)
            )
    
        # Wait for all processes to finish and check for errors
        for future in concurrent.futures.as_completed(futures):
            # This will raise any exceptions that occurred during processing
            future.result()

def _convert_page_to_image(
    raw_pdf_bytes: bytes,
    resolution: int,
    output_directory: str,
    page_number: int
) -> None:
    """
    Convert a single PDF page to a TIFF image.
    
    This is an internal function that should not be called directly.
    Use convert_pdf_to_tiff instead.
    
    Args:
        raw_pdf_bytes: The raw PDF file content
        resolution: The DPI resolution for the output image
        output_directory: Directory where the image will be saved
        page_number: The zero-based index of the page to convert
    """
    # Convert the current page to an image
    images = convert_from_bytes(
        raw_pdf_bytes,
        dpi=resolution,
        first_page=page_number + 1,
        last_page=page_number + 1,
        grayscale=True,
    )
    image = images[0]

    # Save the image with sequential numbering
    image_path = os.path.join(output_directory, f"page_{page_number+1:04}.tiff")
    image.save(image_path, "TIFF", dpi=(resolution, resolution))
    print(f"Saved page {page_number+1} as {image_path}")

def _parse_arguments() -> argparse.Namespace:
    """
    Parse and validate command line arguments.
    
    This is an internal function that should not be called directly.
    
    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Convert PDF pages to TIFF images with multi-threading support.'
    )
    
    # Add required arguments
    parser.add_argument('pdf_file',
                       help='Path to the input PDF file')
    parser.add_argument('-o', '--output',
                       required=True,
                       help='Directory where TIFF images will be saved')
    
    # Add optional arguments
    parser.add_argument('-r', '--resolution',
                       type=int,
                       default=300,
                       help='Resolution in DPI for the output images (default: 300)')
    
    return parser.parse_args()

def main() -> None:
    """Main entry point for command-line usage."""
    # Parse command-line arguments
    args = _parse_arguments()
    
    # Call the main function
    try:
        extract_tiff_from_pdf(args.pdf_file, args.output, args.resolution)
    except Exception as e:
        print(f"Error processing PDF: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()