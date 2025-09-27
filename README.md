## utils/get_symbols_async.py
### Usage
1. **Fetch all symbols and save (without overwriting):**
```bash
python utils/get_symbols_async.py
Force overwrite symbols.json with all symbols:

python utils/get_symbols_async.py --force
Test with only the top 10 symbols:

python utils/get_symbols_async.py --force --limit 10
Update top_symbols in config.json based on "add_symbols":

python utils/get_symbols_async.py --update-top
Combined: fetch 50 symbols and update top_symbols based on "add_symbols":

python utils/get_symbols_async.py --force --limit 50 --update-top

This version is properly formatted with headings, code blocks, and clear step-by-step instructions.



start - venv\Scripts\activate.bat

python utils/get_symbols_async.py --force
python utils/get_symbols_async.py --force --limit 300