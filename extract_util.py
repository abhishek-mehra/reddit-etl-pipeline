import praw
import os
from dotenv import load_dotenv
import sys

load_dotenv()
app_client_id = os.getenv("APP_ID")
app_client_secret = os.getenv("APP_SECRET")
app_user_agent = os.getenv("APP_USER_AGENT")

reddit = praw.Reddit(
    client_id=app_client_id,
    client_secret=app_client_secret,
    user_agent=app_user_agent,
)

def reddit_instance():
    try:
        reddit = praw.Reddit(
            client_id=app_client_id,
            client_secret=app_client_secret,
            password="PASSWORD",
            user_agent=app_user_agent,
            username="USERNAME"
        )

        return reddit

    except Exception as e:
        print(f"Unable to connect. Error: {e}")
        sys.exit(1)


def collect_submission_details(subreddit_name):
    reddit = reddit_instance()
    subreddit = reddit.subreddit(subreddit_name)

    submission_details = []
    for submission in subreddit.hot(limit=10):
        submission_info = {
        "id": submission.id,
        "title": submission.title,
        "author": submission.author.name,
        "post_time": submission.created_utc,
        "upvotes": submission.ups,
        "downvotes": submission.downs,
        "num_comments": submission.num_comments,
        "score": submission.score,
        "comment_karma": 0,
        "first_level_comments_count": 0,
        "second_level_comments_count": 0,
        }

        # Retrieve redditor's karma breakdown
        if submission.author:
            redditor = reddit.redditor(submission.author.name)
            submission_info["comment_karma"] = redditor.comment_karma
            
        # Count first level comments
        submission_info["first_level_comments_count"] = len(submission.comments)
    
        # Count second level comments
        submission.comments.replace_more(limit=None)
        for top_level_comment in submission.comments:
            submission_info["second_level_comments_count"] += len(top_level_comment.replies)

        submission_details.append(submission_info)
    
    return submission_details
