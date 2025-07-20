#!/usr/bin/env python3
"""
Bluesky Post Scraper

This script scrapes posts from a specific Bluesky account using the AT Protocol.
Configure the target account in the .env file.
"""

import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from atproto import Client
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class BlueskyPostScraper:
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        """Initialize the Bluesky scraper."""
        self.client = Client()
        self.authenticated = False

        # Try to authenticate if credentials are provided
        if username and password:
            try:
                self.client.login(username, password)
                self.authenticated = True
                print(f"Successfully authenticated as @{username}")
            except Exception as e:
                print(f"Authentication failed: {e}")
                print(
                    "Continuing without authentication (limited access to public posts only)"
                )
        else:
            print("No credentials provided. Will attempt to access public posts only.")

    def get_user_handle_from_url(self, profile_url: str) -> str:
        if "bsky.app/profile/" in profile_url:
            return profile_url.split("bsky.app/profile/")[-1]
        return profile_url

    def get_user_posts(self, handle: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Fetch posts from a specific user's timeline.

        Args:
            handle: The user's handle (e.g., 'reformexposed.bsky.social')
            limit: Maximum number of posts to fetch (None for all posts)

        Returns:
            List of post dictionaries
        """
        posts = []
        cursor = None
        posts_fetched = 0
        page_count = 0

        try:
            # First, get the profile to verify the user exists
            profile_response = self.client.get_profile(actor=handle)
            print(
                f"Found profile: {profile_response.display_name} (@{profile_response.handle})"
            )

            if limit is None:
                print("Fetching ALL posts from this account...")
            else:
                print(f"Fetching up to {limit} posts...")

            while True:
                # Check if we've reached the limit
                if limit is not None and posts_fetched >= limit:
                    break

                # Calculate how many posts to fetch in this batch
                if limit is not None:
                    batch_size = min(
                        100, limit - posts_fetched
                    )  # Increased batch size for efficiency
                else:
                    batch_size = 100  # Max batch size when fetching all posts

                # Fetch posts using get_author_feed
                response = self.client.get_author_feed(
                    actor=handle, limit=batch_size, cursor=cursor
                )

                if not response.feed:
                    print("No more posts found.")
                    break

                page_count += 1
                posts_in_this_batch = 0

                # Process each post in the response
                for feed_item in response.feed:
                    if limit is not None and posts_fetched >= limit:
                        break

                    post = feed_item.post

                    # Extract post ID from URI for the Bluesky post URL
                    post_uri = getattr(post, "uri", "")
                    post_id = post_uri.split("/")[-1] if post_uri else ""
                    author_handle = getattr(post.author, "handle", "")
                    bluesky_post_url = (
                        f"https://bsky.app/profile/{author_handle}/post/{post_id}"
                        if post_id and author_handle
                        else ""
                    )

                    # Extract only the required fields
                    post_data = {
                        "uri": post_uri,
                        "created_at": getattr(post.record, "created_at", "")
                        if hasattr(post, "record")
                        else "",
                        "text": getattr(post.record, "text", "")
                        if hasattr(post, "record")
                        else "",
                        "bluesky_url": bluesky_post_url,
                        "link_url": "",
                        "link_title": "",
                        "link_description": "",
                    }

                    # Handle external links if present
                    if hasattr(post, "record") and hasattr(post.record, "embed"):
                        embed = post.record.embed
                        if (
                            embed
                            and hasattr(embed, "external")
                            and getattr(embed, "external", None)
                        ):
                            post_data["link_url"] = getattr(embed.external, "uri", "")
                            post_data["link_title"] = getattr(
                                embed.external, "title", ""
                            )
                            post_data["link_description"] = getattr(
                                embed.external, "description", ""
                            )

                    posts.append(post_data)
                    posts_fetched += 1
                    posts_in_this_batch += 1

                # Update cursor for pagination
                cursor = getattr(response, "cursor", None)
                if not cursor:
                    print("Reached the end of available posts.")
                    break

                print(
                    f"Page {page_count}: Fetched {posts_in_this_batch} posts (Total: {posts_fetched})"
                )

                # Be respectful with rate limiting
                time.sleep(1)  # Slightly longer delay for large scraping operations

        except Exception as e:
            print(f"Error fetching posts: {e}")
            if not self.authenticated:
                print("Try running with authentication for better access to posts.")

        print(f"\nCompleted! Total posts fetched: {posts_fetched}")
        return posts

    def save_posts_to_csv(self, posts: List[Dict], filename: Optional[str] = None):
        if not posts:
            print("No posts to save.")
            return

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"bluesky_posts_{timestamp}.csv"

        df = pd.DataFrame(posts)
        df.to_csv(filename, index=False, encoding="utf-8")
        print(f"Saved {len(posts)} posts to {filename}")

    def save_posts_to_json(self, posts: List[Dict], filename: Optional[str] = None):
        if not posts:
            print("No posts to save.")
            return

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"bluesky_posts_{timestamp}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(posts, f, indent=2, ensure_ascii=False, default=str)
        print(f"Saved {len(posts)} posts to {filename}")

    def print_post_summary(self, posts: List[Dict]):
        if not posts:
            print("No posts found.")
            return

        print("\n=== Post Summary ===")
        print(f"Total posts scraped: {len(posts)}")

        if posts:
            # Sort by creation date
            sorted_posts = sorted(posts, key=lambda x: x["created_at"], reverse=True)
            latest_post = sorted_posts[0]
            oldest_post = sorted_posts[-1]

            print(f"Latest post: {latest_post['created_at']}")
            print(f"Oldest post: {oldest_post['created_at']}")

            # Count posts with links
            posts_with_links = sum(1 for post in posts if post.get("link_url", ""))

            print(f"Posts with links: {posts_with_links}")

            # Show a few recent posts
            print("\n=== Recent Posts ===")
            for i, post in enumerate(sorted_posts[:3]):
                print(f"\nPost {i + 1}:")
                print(f"URI: {post['uri']}")
                print(f"Bluesky URL: {post['bluesky_url']}")
                print(f"Date: {post['created_at']}")
                print(
                    f"Text: {post['text'][:200]}{'...' if len(post['text']) > 200 else ''}"
                )
                if post.get("link_url"):
                    print(f"Link: {post['link_url']}")
                    if post.get("link_title"):
                        print(f"Link Title: {post['link_title']}")


def main():
    # Load credentials from environment variables (set in .env file)
    username = os.getenv("BLUESKY_USERNAME")
    password = os.getenv("BLUESKY_PASSWORD")
    target_account = os.getenv("TARGET_ACCOUNT")
    post_limit_str = os.getenv("POST_LIMIT")

    # Parse post limit - if set to "ALL" or not set, fetch all posts
    if post_limit_str and post_limit_str.upper() != "ALL":
        try:
            post_limit = int(post_limit_str)
        except ValueError:
            print(
                f"Invalid POST_LIMIT value: {post_limit_str}. Fetching all posts instead."
            )
            post_limit = None
    else:
        post_limit = None  # Fetch all posts

    # Check required environment variables
    if not username or not password:
        print("Error: BLUESKY_USERNAME and BLUESKY_PASSWORD must be set in .env file")
        print("Please check your .env file and make sure it contains:")
        print("BLUESKY_USERNAME=your-handle.bsky.social")
        print("BLUESKY_PASSWORD=your-app-password")
        return

    if not target_account:
        print("Error: TARGET_ACCOUNT must be set in .env file")
        print(
            "Please add TARGET_ACCOUNT=account-to-scrape.bsky.social to your .env file"
        )
        return

    scraper = BlueskyPostScraper(username, password)

    # Target account
    profile_url = f"https://bsky.app/profile/{target_account}"
    handle = scraper.get_user_handle_from_url(profile_url)

    print(f"Scraping posts from: @{handle}")
    if post_limit is None:
        print("Fetching ALL posts from this account (this may take a while)...")
    else:
        print(f"Fetching up to {post_limit} posts...")
    print("This may take a moment...")

    # Fetch posts
    posts = scraper.get_user_posts(handle, limit=post_limit)

    if posts:
        scraper.print_post_summary(posts)

        # Generate filename with target account name and timestamp
        account_safe = target_account.replace(".", "_").replace("@", "")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"{account_safe}_posts_{timestamp}.csv"
        json_filename = f"{account_safe}_posts_{timestamp}.json"

        # Save to files
        scraper.save_posts_to_csv(posts, csv_filename)
        scraper.save_posts_to_json(posts, json_filename)

        print(f"\nScraping completed! Found {len(posts)} total posts.")
        print("Check the generated files for the full data:")
        print(f"  - {csv_filename}")
        print(f"  - {json_filename}")
    else:
        print("No posts found or an error occurred.")


if __name__ == "__main__":
    main()
