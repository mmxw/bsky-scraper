# Bluesky Authentication Setup

To use this Bluesky scraper, you need to authenticate with your Bluesky account.

## Step 1: Get Your App Password

1. Log into your Bluesky account at https://bsky.app
2. Go to Settings → App Passwords: https://bsky.app/settings/app-passwords
3. Click "Add App Password"
4. Give it a name like "Python Scraper"
5. Copy the generated password (it looks like: `abcd-efgh-ijkl-mnop`)

## Step 2: Set Your Credentials

You have three options:

### Option 1: Environment Variables (Recommended)
```bash
export BLUESKY_USERNAME="your-handle.bsky.social"
export BLUESKY_PASSWORD="your-app-password"
python scrape_bsky.py
```

### Option 2: Edit the Script
Uncomment and fill in these lines in `scrape_bsky.py`:
```python
username = "your-handle.bsky.social"
password = "your-app-password"
```

### Option 3: Command Line (if we add that feature)
```bash
python scrape_bsky.py --username "your-handle.bsky.social" --password "your-app-password"
```

## Important Notes

- **Never use your regular Bluesky password** - only use app passwords
- **Keep your app password secure** - treat it like a password
- You can revoke app passwords anytime from the settings page
- Authentication is required to access most posts due to Bluesky's API restrictions

## Example Usage

After setting up authentication:
```bash
export BLUESKY_USERNAME="myhandle.bsky.social"
export BLUESKY_PASSWORD="abcd-efgh-ijkl-mnop"
python scrape_bsky.py
```

This will scrape posts from @reformexposed.bsky.social and save them to CSV and JSON files.
