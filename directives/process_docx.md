# Directive: Process DOCX Upload

## Goal
When the user uploads or provides a `.docx` file, extract the content and use it as input for downstream tasks (writing, SEO, summarizing, rewriting, etc.).

## Input
- A `.docx` file path provided by the user (e.g. dragged into the chat or placed in `.tmp/`)

## Steps

1. **Read the file** using `execution/read_docx.py`
   ```
   python execution/read_docx.py <path_to_file>
   ```
   Output: plain text of the document, printed to stdout.

2. **Parse the content** — understand what the document is about:
   - Is it a blog post, brief, product description, internal doc?
   - What's the main topic or goal?

3. **Confirm with the user** what they want done with it:
   - Rewrite / improve?
   - SEO-optimize?
   - Summarize?
   - Extract specific info?
   - Use as input for another directive?

4. **Execute** the relevant next directive based on user intent.

## Output
Depends on what the user wants. Default: ask before acting.

## Edge Cases
- File not found → ask user to provide correct path or drop the file in `.tmp/`
- File is scanned PDF disguised as docx → script will return empty text, notify user
- Very large files (>50 pages) → process in chunks if needed
- Password-protected docx → script will error, notify user

## Notes
- Always store uploaded files in `.tmp/` during processing
- Never overwrite the original file
