import glob

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import os
from pyspark.sql.types import *
from pyspark.sql.functions import col

# Define routes
DATA_DIR = "/opt/airflow/dags/data/"
RAW_FILE = os.path.join(DATA_DIR, "2015-03-01-17.json")
EXTRACT_FILE = os.path.join(DATA_DIR, "extract.json")  # Đổi sang .json
TRANSFORMED_FILE = os.path.join(DATA_DIR, "transformed.json")  # Đổi sang .json

# Extract function
def extract():
    # Đọc file JSON thô
    df = pd.read_json(RAW_FILE, lines=True)
    # Lưu lại dưới dạng JSON thay vì CSV
    df.to_json(EXTRACT_FILE, orient="records", lines=True)

# Transform function
def transform():
    spark = SparkSession.builder.appName("GitHubTransform").getOrCreate()

    # Định nghĩa schema giống như trước
    schemaType = StructType([
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("actor", StructType([
            StructField("id", IntegerType(), True),
            StructField("login", StringType(), True),
            StructField("gravatar_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("avatar_url", StringType(), True),
        ]), True),
        StructField("repo", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True),
        ]), True),
        StructField("payload", StructType([
            StructField("action", StringType(), True),
            StructField("comment", StructType([
                StructField("url", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("diff_hunk", StringType(), True),
                StructField("path", StringType(), True),
                StructField("position", IntegerType(), True),
                StructField("original_position", IntegerType(), True),
                StructField("commit_id", StringType(), True),
                StructField("original_commit_id", StringType(), True),
                StructField("user", StructType([
                    StructField("login", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("avatar_url", StringType(), True),
                    StructField("gravatar_id", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("html_url", StringType(), True),
                    StructField("followers_url", StringType(), True),
                    StructField("following_url", StringType(), True),
                    StructField("gists_url", StringType(), True),
                    StructField("starred_url", StringType(), True),
                    StructField("subscriptions_url", StringType(), True),
                    StructField("organizations_url", StringType(), True),
                    StructField("repos_url", StringType(), True),
                    StructField("events_url", StringType(), True),
                    StructField("received_events_url", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("site_admin", BooleanType(), True)
                ]), True),
                StructField("body", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("pull_request_url", StringType(), True),
                StructField("_links", StructType([
                    StructField("self", StructType([StructField("href", StringType(), True)]), True),
                    StructField("html", StructType([StructField("href", StringType(), True)]), True),
                    StructField("pull_request", StructType([StructField("href", StringType(), True)]), True)
                ]), True)
            ]), True),
            StructField("pull_request", StructType([
                StructField("url", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("html_url", StringType(), True),
                StructField("diff_url", StringType(), True),
                StructField("patch_url", StringType(), True),
                StructField("issue_url", StringType(), True),
                StructField("number", IntegerType(), True),
                StructField("state", StringType(), True),
                StructField("locked", BooleanType(), True),
                StructField("title", StringType(), True),
                StructField("user", StructType([
                    StructField("login", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("avatar_url", StringType(), True),
                    StructField("gravatar_id", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("html_url", StringType(), True),
                    StructField("followers_url", StringType(), True),
                    StructField("following_url", StringType(), True),
                    StructField("gists_url", StringType(), True),
                    StructField("starred_url", StringType(), True),
                    StructField("subscriptions_url", StringType(), True),
                    StructField("organizations_url", StringType(), True),
                    StructField("repos_url", StringType(), True),
                    StructField("events_url", StringType(), True),
                    StructField("received_events_url", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("site_admin", BooleanType(), True)
                ]), True),
                StructField("body", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
                StructField("closed_at", StringType(), True),
                StructField("merged_at", StringType(), True),
                StructField("merge_commit_sha", StringType(), True),
                StructField("assignee", StructType([
                    StructField("login", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("avatar_url", StringType(), True),
                    StructField("gravatar_id", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("html_url", StringType(), True),
                    StructField("followers_url", StringType(), True),
                    StructField("following_url", StringType(), True),
                    StructField("gists_url", StringType(), True),
                    StructField("starred_url", StringType(), True),
                    StructField("subscriptions_url", StringType(), True),
                    StructField("organizations_url", StringType(), True),
                    StructField("repos_url", StringType(), True),
                    StructField("events_url", StringType(), True),
                    StructField("received_events_url", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("site_admin", BooleanType(), True)
                ]), True),
                StructField("milestone", StructType([
                    StructField("url", StringType(), True),
                    StructField("html_url", StringType(), True),
                    StructField("labels_url", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("number", IntegerType(), True),
                    StructField("title", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("creator", StructType([
                        StructField("login", StringType(), True),
                        StructField("id", IntegerType(), True),
                        StructField("avatar_url", StringType(), True),
                        StructField("gravatar_id", StringType(), True),
                        StructField("url", StringType(), True),
                        StructField("html_url", StringType(), True),
                        StructField("followers_url", StringType(), True),
                        StructField("following_url", StringType(), True),
                        StructField("gists_url", StringType(), True),
                        StructField("starred_url", StringType(), True),
                        StructField("subscriptions_url", StringType(), True),
                        StructField("organizations_url", StringType(), True),
                        StructField("repos_url", StringType(), True),
                        StructField("events_url", StringType(), True),
                        StructField("received_events_url", StringType(), True),
                        StructField("type", StringType(), True),
                        StructField("site_admin", BooleanType(), True)
                    ]), True),
                    StructField("open_issues", IntegerType(), True),
                    StructField("closed_issues", IntegerType(), True),
                    StructField("state", StringType(), True),
                    StructField("created_at", StringType(), True),
                    StructField("updated_at", StringType(), True),
                    StructField("due_on", StringType(), True),
                    StructField("closed_at", StringType(), True)
                ]), True),
                StructField("commits_url", StringType(), True),
                StructField("review_comments_url", StringType(), True),
                StructField("review_comment_url", StringType(), True),
                StructField("comments_url", StringType(), True),
                StructField("statuses_url", StringType(), True),
                StructField("head", StructType([
                    StructField("label", StringType(), True),
                    StructField("ref", StringType(), True),
                    StructField("sha", StringType(), True),
                    StructField("user", StructType([
                        StructField("login", StringType(), True),
                        StructField("id", IntegerType(), True),
                        StructField("avatar_url", StringType(), True),
                        StructField("gravatar_id", StringType(), True),
                        StructField("url", StringType(), True),
                        StructField("html_url", StringType(), True),
                        StructField("followers_url", StringType(), True),
                        StructField("following_url", StringType(), True),
                        StructField("gists_url", StringType(), True),
                        StructField("starred_url", StringType(), True),
                        StructField("subscriptions_url", StringType(), True),
                        StructField("organizations_url", StringType(), True),
                        StructField("repos_url", StringType(), True),
                        StructField("events_url", StringType(), True),
                        StructField("received_events_url", StringType(), True),
                        StructField("type", StringType(), True),
                        StructField("site_admin", BooleanType(), True)
                    ]), True),
                    StructField("repo", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                        StructField("full_name", StringType(), True),
                        StructField("owner", StructType([
                            StructField("login", StringType(), True),
                            StructField("id", IntegerType(), True),
                            StructField("avatar_url", StringType(), True),
                            StructField("gravatar_id", StringType(), True),
                            StructField("url", StringType(), True),
                            StructField("html_url", StringType(), True),
                            StructField("followers_url", StringType(), True),
                            StructField("following_url", StringType(), True),
                            StructField("gists_url", StringType(), True),
                            StructField("starred_url", StringType(), True),
                            StructField("subscriptions_url", StringType(), True),
                            StructField("organizations_url", StringType(), True),
                            StructField("repos_url", StringType(), True),
                            StructField("events_url", StringType(), True),
                            StructField("received_events_url", StringType(), True),
                            StructField("type", StringType(), True),
                            StructField("site_admin", BooleanType(), True)
                        ]), True),
                        StructField("private", BooleanType(), True),
                        StructField("html_url", StringType(), True),
                        StructField("description", StringType(), True),
                        StructField("fork", BooleanType(), True),
                        StructField("url", StringType(), True),
                        StructField("forks_url", StringType(), True),
                        StructField("keys_url", StringType(), True),
                        StructField("collaborators_url", StringType(), True),
                        StructField("teams_url", StringType(), True),
                        StructField("hooks_url", StringType(), True),
                        StructField("issue_events_url", StringType(), True),
                        StructField("events_url", StringType(), True),
                        StructField("assignees_url", StringType(), True),
                        StructField("branches_url", StringType(), True),
                        StructField("tags_url", StringType(), True),
                        StructField("blobs_url", StringType(), True),
                        StructField("git_tags_url", StringType(), True),
                        StructField("git_refs_url", StringType(), True),
                        StructField("trees_url", StringType(), True),
                        StructField("statuses_url", StringType(), True),
                        StructField("languages_url", StringType(), True),
                        StructField("stargazers_url", StringType(), True),
                        StructField("contributors_url", StringType(), True),
                        StructField("subscribers_url", StringType(), True),
                        StructField("subscription_url", StringType(), True),
                        StructField("commits_url", StringType(), True),
                        StructField("git_commits_url", StringType(), True),
                        StructField("comments_url", StringType(), True),
                        StructField("issue_comment_url", StringType(), True),
                        StructField("contents_url", StringType(), True),
                        StructField("compare_url", StringType(), True),
                        StructField("merges_url", StringType(), True),
                        StructField("archive_url", StringType(), True),
                        StructField("downloads_url", StringType(), True),
                        StructField("issues_url", StringType(), True),
                        StructField("pulls_url", StringType(), True),
                        StructField("milestones_url", StringType(), True),
                        StructField("notifications_url", StringType(), True),
                        StructField("labels_url", StringType(), True),
                        StructField("releases_url", StringType(), True),
                        StructField("created_at", StringType(), True),
                        StructField("updated_at", StringType(), True),
                        StructField("pushed_at", StringType(), True),
                        StructField("git_url", StringType(), True),
                        StructField("ssh_url", StringType(), True),
                        StructField("clone_url", StringType(), True),
                        StructField("svn_url", StringType(), True),
                        StructField("homepage", StringType(), True),
                        StructField("size", IntegerType(), True),
                        StructField("stargazers_count", IntegerType(), True),
                        StructField("watchers_count", IntegerType(), True),
                        StructField("language", StringType(), True),
                        StructField("has_issues", BooleanType(), True),
                        StructField("has_downloads", BooleanType(), True),
                        StructField("has_wiki", BooleanType(), True),
                        StructField("has_pages", BooleanType(), True),
                        StructField("forks_count", IntegerType(), True),
                        StructField("mirror_url", StringType(), True),
                        StructField("open_issues_count", IntegerType(), True),
                        StructField("forks", IntegerType(), True),
                        StructField("open_issues", IntegerType(), True),
                        StructField("watchers", IntegerType(), True),
                        StructField("default_branch", StringType(), True)
                    ]), True)
                ]), True),
                StructField("base", StructType([
                    StructField("label", StringType(), True),
                    StructField("ref", StringType(), True),
                    StructField("sha", StringType(), True),
                    StructField("user", StructType([
                        StructField("login", StringType(), True),
                        StructField("id", IntegerType(), True),
                        StructField("avatar_url", StringType(), True),
                        StructField("gravatar_id", StringType(), True),
                        StructField("url", StringType(), True),
                        StructField("html_url", StringType(), True),
                        StructField("followers_url", StringType(), True),
                        StructField("following_url", StringType(), True),
                        StructField("gists_url", StringType(), True),
                        StructField("starred_url", StringType(), True),
                        StructField("subscriptions_url", StringType(), True),
                        StructField("organizations_url", StringType(), True),
                        StructField("repos_url", StringType(), True),
                        StructField("events_url", StringType(), True),
                        StructField("received_events_url", StringType(), True),
                        StructField("type", StringType(), True),
                        StructField("site_admin", BooleanType(), True)
                    ]), True),
                    StructField("repo", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                        StructField("full_name", StringType(), True),
                        StructField("owner", StructType([
                            StructField("login", StringType(), True),
                            StructField("id", IntegerType(), True),
                            StructField("avatar_url", StringType(), True),
                            StructField("gravatar_id", StringType(), True),
                            StructField("url", StringType(), True),
                            StructField("html_url", StringType(), True),
                            StructField("followers_url", StringType(), True),
                            StructField("following_url", StringType(), True),
                            StructField("gists_url", StringType(), True),
                            StructField("starred_url", StringType(), True),
                            StructField("subscriptions_url", StringType(), True),
                            StructField("organizations_url", StringType(), True),
                            StructField("repos_url", StringType(), True),
                            StructField("events_url", StringType(), True),
                            StructField("received_events_url", StringType(), True),
                            StructField("type", StringType(), True),
                            StructField("site_admin", BooleanType(), True)
                        ]), True),
                        StructField("private", BooleanType(), True),
                        StructField("html_url", StringType(), True),
                        StructField("description", StringType(), True),
                        StructField("fork", BooleanType(), True),
                        StructField("url", StringType(), True),
                        StructField("forks_url", StringType(), True),
                        StructField("keys_url", StringType(), True),
                        StructField("collaborators_url", StringType(), True),
                        StructField("teams_url", StringType(), True),
                        StructField("hooks_url", StringType(), True),
                        StructField("issue_events_url", StringType(), True),
                        StructField("events_url", StringType(), True),
                        StructField("assignees_url", StringType(), True),
                        StructField("branches_url", StringType(), True),
                        StructField("tags_url", StringType(), True),
                        StructField("blobs_url", StringType(), True),
                        StructField("git_tags_url", StringType(), True),
                        StructField("git_refs_url", StringType(), True),
                        StructField("trees_url", StringType(), True),
                        StructField("statuses_url", StringType(), True),
                        StructField("languages_url", StringType(), True),
                        StructField("stargazers_url", StringType(), True),
                        StructField("contributors_url", StringType(), True),
                        StructField("subscribers_url", StringType(), True),
                        StructField("subscription_url", StringType(), True),
                        StructField("commits_url", StringType(), True),
                        StructField("git_commits_url", StringType(), True),
                        StructField("comments_url", StringType(), True),
                        StructField("issue_comment_url", StringType(), True),
                        StructField("contents_url", StringType(), True),
                        StructField("compare_url", StringType(), True),
                        StructField("merges_url", StringType(), True),
                        StructField("archive_url", StringType(), True),
                        StructField("downloads_url", StringType(), True),
                        StructField("issues_url", StringType(), True),
                        StructField("pulls_url", StringType(), True),
                        StructField("milestones_url", StringType(), True),
                        StructField("notifications_url", StringType(), True),
                        StructField("labels_url", StringType(), True),
                        StructField("releases_url", StringType(), True),
                        StructField("created_at", StringType(), True),
                        StructField("updated_at", StringType(), True),
                        StructField("pushed_at", StringType(), True),
                        StructField("git_url", StringType(), True),
                        StructField("ssh_url", StringType(), True),
                        StructField("clone_url", StringType(), True),
                        StructField("svn_url", StringType(), True),
                        StructField("homepage", StringType(), True),
                        StructField("size", IntegerType(), True),
                        StructField("stargazers_count", IntegerType(), True),
                        StructField("watchers_count", IntegerType(), True),
                        StructField("language", StringType(), True),
                        StructField("has_issues", BooleanType(), True),
                        StructField("has_downloads", BooleanType(), True),
                        StructField("has_wiki", BooleanType(), True),
                        StructField("has_pages", BooleanType(), True),
                        StructField("forks_count", IntegerType(), True),
                        StructField("mirror_url", StringType(), True),
                        StructField("open_issues_count", IntegerType(), True),
                        StructField("forks", IntegerType(), True),
                        StructField("open_issues", IntegerType(), True),
                        StructField("watchers", IntegerType(), True),
                        StructField("default_branch", StringType(), True)
                    ]), True)
                ]), True),
                StructField("_links", StructType([
                    StructField("self", StructType([StructField("href", StringType(), True)]), True),
                    StructField("html", StructType([StructField("href", StringType(), True)]), True),
                    StructField("issue", StructType([StructField("href", StringType(), True)]), True),
                    StructField("comments", StructType([StructField("href", StringType(), True)]), True),
                    StructField("review_comments", StructType([StructField("href", StringType(), True)]), True),
                    StructField("review_comment", StructType([StructField("href", StringType(), True)]), True),
                    StructField("commits", StructType([StructField("href", StringType(), True)]), True),
                    StructField("statuses", StructType([StructField("href", StringType(), True)]), True)
                ]), True)
            ]), True)
        ]), True),
        StructField("public", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("org", StructType([
            StructField("id", IntegerType(), True),
            StructField("login", StringType(), True),
            StructField("gravatar_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("avatar_url", StringType(), True)
        ]), True)
    ])

    # Đọc file JSON thay vì CSV
    df = spark.read.schema(schemaType).json(EXTRACT_FILE)

    # Lọc và đổi tên cột
    df_filtered = df.select(
        "id",
        "type",
        col("actor.login").alias("actor_login"),
        col("repo.name").alias("repo_name"),
        "created_at"
    )

    # Ghi kết quả dưới dạng JSON thay vì CSV
    df_filtered.write.json(TRANSFORMED_FILE, mode="overwrite")
    spark.stop()

# Load function
def load():
    json_files = glob.glob(os.path.join(TRANSFORMED_FILE, "part-*.json"))
    df_list = [pd.read_json(file, lines=True) for file in json_files]
    df = pd.concat(df_list, ignore_index=True)
    engine = create_engine('postgresql://admin:admin@de_psql:5432/postgres')
    df.to_sql('github_events', engine, if_exists='replace', index=False)
    # df = pd.read_json(TRANSFORMED_FILE, lines=True)
    # engine = create_engine("postgresql://admin:admin@de_psql:5434/postgres")
    # df.to_sql('github_events', engine, if_exists='replace', index=False)

# Define DAG
with DAG(
    dag_id="github_etl",
    schedule_interval="@daily",
    start_date=datetime(2025, 3, 18),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task