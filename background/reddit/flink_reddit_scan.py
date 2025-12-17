"""
Reddit Environmental Scan: Flink Optimization Discussions
Searches r/apacheflink and r/dataengineering for relevant posts.

Setup:
1. pip install praw --break-system-packages
2. Create Reddit app at https://www.reddit.com/prefs/apps
   - Choose "script" type
   - Note your client_id (under app name) and client_secret
3. Set environment variables or edit credentials below

Output: CSV with title, score, comments, date, url, subreddit
"""

import praw
import csv
import os
from datetime import datetime

# --- CREDENTIALS ---
# Option 1: Set these environment variables
# Option 2: Replace with your values directly
CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "YOUR_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "YOUR_CLIENT_SECRET")
USER_AGENT = "flink-research-scan by /u/sooperD00"

# --- CONFIG ---
SUBREDDITS = ["apacheflink", "dataengineering"]
KEYWORDS = [
    "optimization",
    "cost",
    "autoscaler",
    "tuning",
    "parallelism",
    "rightsizing",
    "backpressure",
    "taskmanager",
    "memory",
    "scaling",
    "kubernetes cost",
    "reduce cost",
    "overprovisioned",
]
OUTPUT_FILE = "flink_reddit_scan.csv"
POSTS_PER_SEARCH = 50  # Reddit API limit per query


def init_reddit():
    return praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT,
    )


def search_subreddit(reddit, subreddit_name, keyword):
    """Search a subreddit for a keyword, return post data."""
    results = []
    subreddit = reddit.subreddit(subreddit_name)
    
    try:
        for post in subreddit.search(keyword, limit=POSTS_PER_SEARCH, time_filter="year"):
            results.append({
                "subreddit": subreddit_name,
                "keyword": keyword,
                "title": post.title,
                "score": post.score,
                "num_comments": post.num_comments,
                "created": datetime.fromtimestamp(post.created_utc).strftime("%Y-%m-%d"),
                "url": f"https://reddit.com{post.permalink}",
                "selftext_preview": (post.selftext[:200] + "...") if post.selftext else "",
            })
    except Exception as e:
        print(f"  Error searching r/{subreddit_name} for '{keyword}': {e}")
    
    return results


def dedupe_by_url(results):
    """Remove duplicate posts (same post matched by multiple keywords)."""
    seen = set()
    deduped = []
    for r in results:
        if r["url"] not in seen:
            seen.add(r["url"])
            deduped.append(r)
    return deduped


def main():
    print("Initializing Reddit connection...")
    reddit = init_reddit()
    
    all_results = []
    
    for subreddit in SUBREDDITS:
        print(f"\nSearching r/{subreddit}...")
        for keyword in KEYWORDS:
            print(f"  Keyword: {keyword}")
            results = search_subreddit(reddit, subreddit, keyword)
            print(f"    Found {len(results)} posts")
            all_results.extend(results)
    
    # Dedupe
    deduped = dedupe_by_url(all_results)
    print(f"\nTotal unique posts: {len(deduped)}")
    
    # Sort by score (most engagement first)
    deduped.sort(key=lambda x: x["score"], reverse=True)
    
    # Write CSV
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "subreddit", "title", "score", "num_comments", "created", "url", "keyword", "selftext_preview"
        ])
        writer.writeheader()
        writer.writerows(deduped)
    
    print(f"Wrote {len(deduped)} posts to {OUTPUT_FILE}")
    
    # Quick summary
    print("\n--- TOP 10 BY ENGAGEMENT ---")
    for post in deduped[:10]:
        print(f"[{post['score']:3d}⬆ {post['num_comments']:2d}💬] r/{post['subreddit']}: {post['title'][:60]}")


if __name__ == "__main__":
    main()
