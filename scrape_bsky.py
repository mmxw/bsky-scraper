#!/usr/bin/env python3
"""
Bluesky Post Scraper

This script scrapes posts from a specific Bluesky account using the AT Protocol.
Configure the target account in the .env file.
"""

import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any
from atproto import Client
import time
import os
from dotenv import load_dotenv
import re
import requests
from bs4 import BeautifulSoup
import spacy
from spacy.matcher import Matcher

# Load environment variables from .env file
load_dotenv()


class LocationExtractor:
    """Extract location information and person names from text using spaCy NLP."""

    def __init__(self):
        try:
            # Try to load the English model
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            print("spaCy English model not found. Please install it with:")
            print("python -m spacy download en_core_web_sm")
            raise

        # Add custom UK location patterns for better coverage
        self.matcher = Matcher(self.nlp.vocab)

        # Comprehensive list of UK locations for pattern matching
        self.uk_cities = {
            "london", "birmingham", "manchester", "glasgow", "liverpool", "leeds", 
            "sheffield", "edinburgh", "bristol", "cardiff", "belfast", "newcastle",
            "leicester", "coventry", "bradford", "nottingham", "hull", "plymouth",
            "stoke-on-trent", "wolverhampton", "derby", "southampton", "portsmouth",
            "brighton", "aberdeen", "northampton", "norwich", "luton", "solihull",
            "sunderland", "poole", "milton keynes", "slough", "bournemouth", "reading",
            "peterborough", "warrington", "stockport", "rochdale", "rotherham", "oldham",
            "blackpool", "middlesbrough", "huddersfield", "oxford", "cambridge", "ipswich",
            "york", "gloucester", "watford", "chester", "exeter", "doncaster", "crawley",
            "blackburn", "basildon", "burnley", "bolton", "gillingham", "maidstone",
            "high wycombe", "worthing", "southend-on-sea", "chelmsford", "colchester",
            "preston", "st albans", "harrogate", "eastbourne", "grimsby", "bath",
            "worcester", "scunthorpe", "stevenage", "hemel hempstead", "basingstoke"
        }

        self.uk_counties = {
            "yorkshire", "lancashire", "devon", "cornwall", "dorset", "somerset", "kent",
            "sussex", "surrey", "essex", "hampshire", "berkshire", "oxfordshire",
            "buckinghamshire", "hertfordshire", "bedfordshire", "cambridgeshire", "suffolk",
            "norfolk", "lincolnshire", "nottinghamshire", "leicestershire", "warwickshire",
            "northamptonshire", "gloucestershire", "wiltshire", "worcestershire",
            "herefordshire", "shropshire", "staffordshire", "derbyshire", "cheshire",
            "merseyside", "greater manchester", "west yorkshire", "south yorkshire",
            "east yorkshire", "north yorkshire", "west midlands", "east midlands",
            "south west", "south east", "north west", "north east", "wales", "scotland",
            "northern ireland", "cumbria", "northumberland", "durham", "tyne and wear",
            "cleveland", "avon", "humberside"
        }

        # UK-specific location patterns that might not be in the default model
        uk_patterns = [
            # UK constituencies and councils
            [
                {
                    "LOWER": {
                        "IN": ["cheshire", "bath", "north", "south", "east", "west"]
                    }
                },
                {"LOWER": {"IN": ["and", "west", "east", "north", "south"]}, "OP": "?"},
                {"LOWER": {"IN": ["chester", "somerset", "lincolnshire", "yorkshire"]}},
            ],
            # Places with "upon" or "on"
            [{"IS_ALPHA": True}, {"LOWER": {"IN": ["upon", "on"]}}, {"IS_ALPHA": True}],
            # Royal/City/County prefixes
            [
                {"LOWER": {"IN": ["royal", "city", "county"]}},
                {"LOWER": {"IN": ["borough", "of"]}},
                {"IS_ALPHA": True, "OP": "+"},
            ],
        ]

        self.matcher.add("UK_LOCATIONS", uk_patterns)

    def extract_locations_and_persons_from_text(
        self, text: str
    ) -> tuple[List[str], List[Dict[str, str]]]:
        """
        Extract locations and person names with roles from text using spaCy NER.

        Returns:
            tuple: (locations, person_data_list)
            person_data_list contains dicts with 'name' and 'role' keys
        """
        if not text:
            return [], []

        # Process text with spaCy
        doc = self.nlp(text)

        locations = set()
        persons = []

        # Extract named entities
        for ent in doc.ents:
            entity_text = ent.text.strip()

            # Skip very short entities or those with just numbers
            if len(entity_text) <= 2 or entity_text.isdigit():
                continue

            if ent.label_ in [
                "GPE",
                "LOC",
            ]:  # GPE = Geopolitical entity, LOC = Location
                locations.add(entity_text)
            elif ent.label_ == "PERSON":
                # Extract role information for this person
                role = self._extract_person_role(entity_text, text, ent.start_char, ent.end_char)
                persons.append({
                    "name": entity_text,
                    "role": role
                })

        # Also check for custom UK location patterns
        matches = self.matcher(doc)
        for match_id, start, end in matches:
            span = doc[start:end]
            location_text = span.text.strip()
            if len(location_text) > 2:
                locations.add(location_text)

        # Additional word-level matching for UK locations that spaCy might miss
        text_lower = text.lower()
        
        # Check for UK cities
        for city in self.uk_cities:
            if city in text_lower:
                # Make sure it's a whole word match
                import re
                pattern = r'\b' + re.escape(city) + r'\b'
                if re.search(pattern, text_lower):
                    # Find the original case version
                    original_match = re.search(pattern, text, re.IGNORECASE)
                    if original_match:
                        locations.add(original_match.group())

        # Check for UK counties
        for county in self.uk_counties:
            if county in text_lower:
                # Make sure it's a whole word match
                import re
                pattern = r'\b' + re.escape(county) + r'\b'
                if re.search(pattern, text_lower):
                    # Find the original case version
                    original_match = re.search(pattern, text, re.IGNORECASE)
                    if original_match:
                        locations.add(original_match.group())

        # Additional filtering for UK-specific locations
        filtered_locations = []
        for location in locations:
            if self._is_likely_uk_location(location):
                filtered_locations.append(location)

        return filtered_locations, persons

    def _extract_person_role(self, person_name: str, full_text: str, start_pos: int, end_pos: int) -> str:
        """Extract the role (MP, councillor, candidate) for a person from context."""
        # Get context around the person's name (100 chars before and after)
        context_start = max(0, start_pos - 100)
        context_end = min(len(full_text), end_pos + 100)
        context = full_text[context_start:context_end].lower()
        person_lower = person_name.lower()
        
        # Role patterns to look for
        role_patterns = [
            # MP patterns
            (r'\b(?:mp|member of parliament)\s+' + re.escape(person_lower), 'MP'),
            (r'\b' + re.escape(person_lower) + r'\s+(?:mp|member of parliament)\b', 'MP'),
            (r'\b' + re.escape(person_lower) + r'\s+\([^)]*mp[^)]*\)', 'MP'),
            
            # Councillor patterns
            (r'\bcouncillor\s+' + re.escape(person_lower), 'Councillor'),
            (r'\bcllr\s+' + re.escape(person_lower), 'Councillor'),
            (r'\b' + re.escape(person_lower) + r'\s+(?:councillor|cllr)\b', 'Councillor'),
            (r'\b' + re.escape(person_lower) + r'\s+\([^)]*councillor[^)]*\)', 'Councillor'),
            (r'\b' + re.escape(person_lower) + r'\s+\([^)]*cllr[^)]*\)', 'Councillor'),
            
            # Candidate patterns
            (r'\bcandidate\s+' + re.escape(person_lower), 'Candidate'),
            (r'\b' + re.escape(person_lower) + r'\s+candidate\b', 'Candidate'),
            (r'\b' + re.escape(person_lower) + r'\s+\([^)]*candidate[^)]*\)', 'Candidate'),
            (r'\bprospective\s+(?:mp\s+)?candidate\s+' + re.escape(person_lower), 'Candidate'),
            (r'\b' + re.escape(person_lower) + r'\s+(?:stands|standing|runs|running)\s+(?:for|as)', 'Candidate'),
            
            # Leader/Deputy patterns
            (r'\bleader\s+' + re.escape(person_lower), 'Leader'),
            (r'\b' + re.escape(person_lower) + r'\s+leader\b', 'Leader'),
            (r'\bdeputy\s+leader\s+' + re.escape(person_lower), 'Deputy Leader'),
            (r'\b' + re.escape(person_lower) + r'\s+deputy\s+leader\b', 'Deputy Leader'),
        ]
        
        # Check for role patterns
        for pattern, role in role_patterns:
            if re.search(pattern, context, re.IGNORECASE):
                return role
        
        # Check the broader context for political keywords
        political_keywords = ['reform uk', 'reform party', 'election', 'constituency', 'council', 'government', 'political', 'parliament']
        if any(keyword in context for keyword in political_keywords):
            return 'Political Figure'
        
        return 'Person'

    def _is_likely_uk_location(self, location: str) -> bool:
        """Check if a location is likely to be a UK location."""
        location_lower = location.lower()

        # Check if it's in our comprehensive UK location lists
        if location_lower in self.uk_cities or location_lower in self.uk_counties:
            return True

        # Known UK location indicators
        uk_indicators = [
            # Common UK place suffixes and components
            "shire",
            "borough",
            "council",
            "upon",
            "green",
            "common",
            "heath",
            "bridge",
            "cross",
            "gate",
            "town",
            "city",
            "district",
            "ward",
            # UK-specific location patterns
            "cheshire",
            "gloucestershire",
            "worcestershire",
            "staffordshire",
            "derbyshire",
            "nottinghamshire",
            "leicestershire",
            "warwickshire",
        ]

        # Check if location contains UK indicators
        if any(indicator in location_lower for indicator in uk_indicators):
            return True

        # Check for UK postcode pattern
        postcode_pattern = r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2}\b"
        if re.search(postcode_pattern, location, re.IGNORECASE):
            return True

        # If it's a single word and title case, it might be a UK place
        if len(location.split()) == 1 and location.istitle():
            return True

        return False

    def extract_locations_from_text(self, text: str) -> List[str]:
        """
        Extract only locations from text (for backward compatibility).
        """
        locations, _ = self.extract_locations_and_persons_from_text(text)
        return locations

    def fetch_and_extract_from_url(self, url: str) -> tuple[str, List[str]]:
        """Fetch content from URL and extract locations."""
        if not url:
            return "", []

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()

            # Get text content
            text = soup.get_text()

            # Clean up text
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = " ".join(chunk for chunk in chunks if chunk)

            # Extract locations from the content
            locations = self.extract_locations_from_text(text)

            return text[:500], locations  # Return first 500 chars and locations

        except Exception as e:
            print(f"Error fetching URL {url}: {e}")
            return "", []


class BlueskyPostScraper:
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        """Initialize the Bluesky scraper."""
        self.client = Client()
        self.authenticated = False
        self.location_extractor = LocationExtractor()

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
                    post_data: Dict[str, Any] = {
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
                        "text_locations": [],
                        "text_persons": [],
                        "link_locations": [],
                        "link_persons": [],
                        "all_locations": [],
                        "all_persons": [],
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

                    # Extract locations and persons from post text
                    post_text = str(post_data["text"]) if post_data["text"] else ""
                    if post_text:
                        text_locations, text_persons = (
                            self.location_extractor.extract_locations_and_persons_from_text(
                                post_text
                            )
                        )
                        post_data["text_locations"] = text_locations
                        post_data["text_persons"] = text_persons

                    # Extract locations and persons from link title and description
                    link_title = (
                        str(post_data["link_title"]) if post_data["link_title"] else ""
                    )
                    link_desc = (
                        str(post_data["link_description"])
                        if post_data["link_description"]
                        else ""
                    )
                    link_text = f"{link_title} {link_desc}".strip()
                    if link_text:
                        link_locations, link_persons = (
                            self.location_extractor.extract_locations_and_persons_from_text(
                                link_text
                            )
                        )
                        post_data["link_locations"] = link_locations
                        post_data["link_persons"] = link_persons

                    # Combine all unique locations and persons
                    all_locations: set[str] = set()
                    all_persons: List[Dict[str, str]] = []
                    
                    all_locations.update(post_data["text_locations"])
                    all_locations.update(post_data["link_locations"])
                    
                    # Combine person data while preserving roles
                    person_dict = {}
                    for person_data in post_data["text_persons"]:
                        name = person_data["name"]
                        role = person_data["role"]
                        if name not in person_dict or role != "Person":  # Prefer specific roles over generic "Person"
                            person_dict[name] = role
                    
                    for person_data in post_data["link_persons"]:
                        name = person_data["name"]
                        role = person_data["role"]
                        if name not in person_dict or (person_dict[name] == "Person" and role != "Person"):
                            person_dict[name] = role
                    
                    # Convert back to list of dicts
                    all_persons = [{"name": name, "role": role} for name, role in person_dict.items()]
                    
                    post_data["all_locations"] = list(all_locations)
                    post_data["all_persons"] = all_persons

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

            # Count posts with locations and persons
            posts_with_locations = sum(
                1 for post in posts if post.get("all_locations", [])
            )
            posts_with_persons = sum(1 for post in posts if post.get("all_persons", []))

            print(f"Posts with links: {posts_with_links}")
            print(f"Posts with location mentions: {posts_with_locations}")
            print(f"Posts with person mentions: {posts_with_persons}")

            # Show most mentioned locations
            all_location_mentions: dict[str, int] = {}
            for post in posts:
                for location in post.get("all_locations", []):
                    all_location_mentions[location] = (
                        all_location_mentions.get(location, 0) + 1
                    )

            # Show most mentioned persons
            all_person_mentions: dict[str, int] = {}
            for post in posts:
                for person in post.get("all_persons", []):
                    # Handle both old format (strings) and new format (dicts)
                    person_name = person if isinstance(person, str) else person.get("name", "")
                    if person_name:
                        all_person_mentions[person_name] = all_person_mentions.get(person_name, 0) + 1

            if all_location_mentions:
                top_locations = sorted(
                    all_location_mentions.items(), key=lambda x: x[1], reverse=True
                )[:10]
                print("\nTop mentioned locations:")
                for location, count in top_locations:
                    print(f"  {location}: {count} mentions")

            if all_person_mentions:
                top_persons = sorted(
                    all_person_mentions.items(), key=lambda x: x[1], reverse=True
                )[:10]
                print("\nTop mentioned persons:")
                for person, count in top_persons:
                    print(f"  {person}: {count} mentions")

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
                if post.get("all_locations"):
                    print(f"Locations mentioned: {', '.join(post['all_locations'])}")
                if post.get("all_persons"):
                    person_list = []
                    for person in post["all_persons"]:
                        if isinstance(person, str):
                            person_list.append(person)
                        else:
                            name = person.get("name", "")
                            role = person.get("role", "")
                            if role:
                                person_list.append(f"{name} ({role})")
                            else:
                                person_list.append(name)
                    print(f"Persons mentioned: {', '.join(person_list)}")


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
