import datetime
import json
import logging
import math

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import os
import re
import io
from collections import Counter

import botocore
import boto3
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from wordcloud import WordCloud
from konoha import WordTokenizer

import html_text

html_text.cleaner.kill_tags = ["code", "blockquote"]

wc_config = dict(
    font_path="/usr/share/fonts/truetype/takao-gothic/TakaoGothic.ttf",
    min_font_size=12,
    width=300,
    height=120,
    mode="RGBA",
    background_color=(0, 0, 0, 0),
)

ssm_client = boto3.client("ssm")
kibela_team_name = os.environ["SSM_KIBELA_TEAM"]
kibela_token_name = os.environ["SSM_KIBELA_TOKEN"]
ssm_response = ssm_client.get_parameters(Names=[kibela_team_name, kibela_token_name])
for param in ssm_response["Parameters"]:
    if param["Name"] == kibela_team_name:
        kibela_team = param["Value"]
    elif param["Name"] == kibela_token_name:
        kibela_token = param["Value"]
if not kibela_team or not kibela_token:
    raise Exception("can't retrieve ssm")

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
public_bucket_name = os.environ["S3_PUBLIC"]
private_bucket_name = os.environ["S3_PRIVATE"]

public_bucket = s3_resource.Bucket(public_bucket_name)
private_bucket = s3_resource.Bucket(private_bucket_name)

# Select your transport with a defined url endpoint
transport = RequestsHTTPTransport(
    url=f"https://{kibela_team}.kibe.la/api/v1",
    headers={
        "Authorization": f"Bearer {kibela_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        # "User-Agent": user_agent
    },
)

# Create a GraphQL client using the defined transport
gql_client = Client(transport=transport, fetch_schema_from_transport=False)

note_id_from_path = gql(
    """
query($path: String!) {
  note: noteFromPath(path: $path) {
    id
    contentUpdatedAt
    isArchived
  }
}
"""
)


note_from_id = gql(
    """
query($id: ID!) {
    note(id: $id) {
        id
        title
        contentHtml
    }
}
"""
)

notes_first = gql(
    """
query {
  notes(first:10, orderBy:{field:CONTENT_UPDATED_AT, direction:ASC}){
    nodes{
      id
      contentUpdatedAt
      isArchived
    }
    pageInfo{
      hasNextPage
      endCursor
    }
    totalCount
  }
}
"""
)

notes_secdond = gql(
    """
query($count: Int, $cursor: String) {
  notes(first:$count, after: $cursor, orderBy:{field:CONTENT_UPDATED_AT, direction:ASC}){
    nodes{
      id
      contentUpdatedAt
      isArchived
    }
  }
}
"""
)

note_detail_from_id = gql(
    """
query($id: ID!) {
  note(id: $id) {
    author {
      realName
      url
    }
    contributors(first: 5) {
      totalCount
      nodes {
        realName
        url
      }
    }
    folder {
      path
      fullName
    }
    groups {
      name
      path
    }
    likers {
      totalCount
    }
    commentsCount
    id
    title
    url
    publishedAt
    contentUpdatedAt
  }
}
"""
)


def get_tfidf_png_key(id_):
    return f"wc_tf_idf/{id_}.png"


def get_tf_tsv_key(id_):
    return f"tf/{id_}.tsv"


def update_tf(id_):
    result = gql_client.execute(note_from_id, variable_values={"id": id_})
    note = result["note"]
    html = note["contentHtml"]
    text = html_text.extract_text(html)
    tokenizer = WordTokenizer("sudachi", mode="C", with_postag=True)
    words = tokenizer.tokenize(text)
    hiragana_re = re.compile("[\u3041-\u309F]+")
    filtered_words = list(
        filter(
            # lambda x: len(x) > 3 or not hiragana_re.fullmatch(x),
            lambda x: len(x) > 3 if hiragana_re.fullmatch(x) else len(x) > 1,
            # map(lambda x: x.normalized_form, filter(lambda x: x.postag in ["名詞", "動詞"], words)),
            map(lambda x: x.surface, filter(lambda x: x.postag in ["名詞"], words)),
        )
    )
    num_words = len(filtered_words)
    word_count = Counter(filtered_words)
    word_freq_list = list(map(lambda k: (k, word_count[k] / num_words), word_count))
    word_freq = dict(word_freq_list)
    tf_tsv_key = get_tf_tsv_key(id_)
    tf_tsv = "\n".join(map(lambda x: "\t".join(map(str, x)), word_freq_list))
    private_bucket.put_object(Body=tf_tsv.encode("utf-8"), Key=tf_tsv_key)
    return word_freq


def get_tfidf_png_url(id_):
    pngkey = get_tfidf_png_key(id_)
    return f"""https://{public_bucket_name}.s3.amazonaws.com/{pngkey}"""


def get_page_ids():
    result = gql_client.execute(notes_first, variable_values={})
    ids_first = result["notes"]["nodes"]
    if result["notes"]["pageInfo"]["hasNextPage"]:
        remain_count = result["notes"]["totalCount"] - len(ids_first)
        second_result = gql_client.execute(
            notes_secdond,
            variable_values={
                "count": remain_count,
                "cursor": result["notes"]["pageInfo"]["endCursor"],
            },
        )
        ids_second = second_result["notes"]["nodes"]
        return ids_first + ids_second
    else:
        return ids_first


def update_tf_s3(id_, content_updated_at):
    tf_tsv_key = get_tf_tsv_key(id_)
    need_update = True
    try:
        obj = private_bucket.Object(tf_tsv_key)
        last_modified = obj.last_modified
        update_datetime = datetime.datetime.fromisoformat(content_updated_at)
        if update_datetime <= last_modified:
            need_update = False
    except Exception as e:
        logger.info(f"update_tf[{id_}] Exception: {e}")
    if need_update:
        logger.info(f"update tf for {id_}")
        update_tf(id_)
    return


def delete_tf_s3(id_):
    tf_tsv_key = get_tf_tsv_key(id_)
    try:
        obj = private_bucket.Object(tf_tsv_key)
        obj.delete()
    except Exception as e:
        logger.error(f"Delete [{id_}] Exception: {e}")


def update_idf():
    ret = s3_client.list_objects_v2(
        Bucket=private_bucket_name, Prefix="tf/", MaxKeys=10
    )
    key_list = list(map(lambda x: x["Key"], ret["Contents"]))
    while ret["IsTruncated"]:
        ret = s3_client.list_objects_v2(
            Bucket=private_bucket_name,
            Prefix="tf/",
            MaxKeys=10,
            ContinuationToken=ret["NextContinuationToken"],
        )
        key_list += list(map(lambda x: x["Key"], ret["Contents"]))

    counter = Counter()

    for key in key_list:
        obj = private_bucket.Object(key)
        ret = obj.get()
        words = list(
            map(
                lambda line: line.decode("utf-8").split("\t")[0],
                ret["Body"].iter_lines(),
            )
        )
        counter.update(words)
    num_files = len(key_list)
    logger.info(f"num_files:{num_files}")
    idf_tsv = "\n".join(
        map(lambda k: f"{k}\t{math.log(num_files / counter[k])}", counter)
    )
    private_bucket.put_object(Body=idf_tsv.encode("utf-8"), Key="idf.tsv")


def to_f_map(tpl):
    return (tpl[0], float(tpl[1]))


def get_idf_from_s3():
    idf_obj = private_bucket.Object("idf.tsv")
    ret = idf_obj.get()
    words = dict(
        map(
            lambda line: to_f_map(line.decode("utf-8").split("\t")),
            ret["Body"].iter_lines(),
        )
    )
    return words


def update_tf_idf_png(id_):
    idf_map = get_idf_from_s3()

    tf_tsv_key = get_tf_tsv_key(id_)
    tf_obj = private_bucket.Object(tf_tsv_key)
    ret = tf_obj.get()
    tf_words = dict(
        map(
            lambda line: to_f_map(line.decode("utf-8").split("\t")),
            ret["Body"].iter_lines(),
        )
    )
    tf_idf = {}
    for word, tf_freq in tf_words.items():
        idf = idf_map.get(word, 0.0)
        tf_idf[word] = tf_freq * idf

    if len(tf_idf) == 0:
        tf_idf["?"] = 1.0

    wc = WordCloud(**wc_config)
    wc.generate_from_frequencies(tf_idf)
    wc_img = wc.to_image()
    logging.info(f"{wc_img=} {wc_img.size}")
    with io.BytesIO() as bio:
        wc_img.save(bio, format="png")
        bio.seek(0)
        pngkey = get_tfidf_png_key(id_)
        public_bucket.upload_fileobj(bio, pngkey)
        logging.info(f"done.")


def update_tf_idf_s3(id_, content_updated_at):
    logger.info(f"update_tf_idf_s3 [{id_}] [{content_updated_at}]")
    tf_tsv_key = get_tf_tsv_key(id_)
    tf_idf_png_key = get_tfidf_png_key(id_)
    need_update = True
    try:
        obj = public_bucket.Object(tf_idf_png_key)
        last_modified = obj.last_modified
        update_datetime = datetime.datetime.fromisoformat(content_updated_at)
        if update_datetime <= last_modified:
            need_update = False
    except Exception as e:
        logger.info(f"update_tf_idf_png[{id_}]: Exception: {e}")
    if need_update:
        logger.info(f"update tf_idf_png: {id_}")
        update_tf_idf_png(id_)
    return


def get_note_id_from_url(url):
    result = gql_client.execute(note_id_from_path, variable_values={"path": url})
    ret = result["note"]
    id_ = ret["id"]
    logger.info(f"{ret['contentUpdatedAt']=} {type(ret['contentUpdatedAt'])}")
    tf_tsv_key = get_tf_tsv_key(id_)
    tf_tsv_obj = private_bucket.Object(tf_tsv_key)
    try:
        tf_tsv_updated_at = tf_tsv_obj.last_modified.isoformat()
    except:
        tf_tsv_updated_at = None
    ret["tfTsvUpdatedAt"] = tf_tsv_updated_at

    tfidf_png_key = get_tfidf_png_key(id_)
    tfidf_png_obj = public_bucket.Object(tfidf_png_key)
    try:
        tfidf_png_updated_at = tfidf_png_obj.last_modified.isoformat()
    except:
        tfidf_png_updated_at = None
    ret["tfidfPngUpdatedAt"] = tfidf_png_updated_at

    return ret


def unfurl_from_id(id_):
    result = gql_client.execute(note_detail_from_id, variable_values={"id": id_})
    logging.info(f"note_detail_from_id: {result=}")
    note = result["note"]
    tfidf_png_url = get_tfidf_png_url(note["id"])
    logging.info(f"{tfidf_png_url=}")

    folder_name = (
        f"""<https://{kibela_team}.kibe.la{note["folder"]["path"]}|{note["folder"]["fullName"]}>"""
        if "folder" in note and note["folder"]
        else "未設定"
    )
    groups = "/".join(
        list(
            map(
                lambda g: f"""<https://{kibela_team}.kibe.la{g["path"]}|{g["name"]}>""",
                note["groups"],
            )
        )
    )
    contributors = "/".join(
        list(
            map(
                lambda c: f"""<{c["url"]}|{c["realName"]}>""",
                note["contributors"]["nodes"],
            )
        )
    )
    attachement = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"""Kibela記事 | <{note["url"]}|*{note["title"]}*>""",
                },
            },
            {"type": "image", "image_url": tfidf_png_url, "alt_text": note["title"]},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"""*作成者:* <{note["author"]["url"]}|{note["author"]["realName"]}>""",
                    },
                    # {
                    #     "type": "image",
                    #     "image_url": note["author"]["avatarImage"]["url"].replace(
                    #         "private", "public"
                    #     ),
                    #     "alt_text": note["author"]["realName"],
                    # },
                    {"type": "mrkdwn", "text": f"""*編集者:* {contributors}"""},
                    {"type": "mrkdwn", "text": f"""*フォルダ:* {folder_name}"""},
                    {"type": "mrkdwn", "text": f"""*グループ:* {groups}"""},
                    {
                        "type": "mrkdwn",
                        "text": f"""*公開日:* <!date^{int(datetime.datetime.fromisoformat(note["publishedAt"]).timestamp())}^{{date}} {{time}}|{note["publishedAt"]}>""",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"""*更新日:* <!date^{int(datetime.datetime.fromisoformat(note["contentUpdatedAt"]).timestamp())}^{{date}} {{time}}|{note["contentUpdatedAt"]}>""",
                    },
                    {"type": "mrkdwn", "text": f"""*コメント数:* {note["commentsCount"]}"""},
                    {
                        "type": "mrkdwn",
                        "text": f"""*イイネ数:* {note["likers"]["totalCount"]}""",
                    },
                ],
            },
        ]
    }
    return attachement

def handler(event, context):
    logger.info(f"step_handler {event=} {context=}")
    action = event["action"]
    if action == "enumerate_notes":
        event["id_list"] = get_page_ids()
    elif action == "get_note_from_url":
        url = event["url"]
        note = get_note_id_from_url(url)
        event["id"] = note["id"]
        event["contentUpdatedAt"] = note["contentUpdatedAt"]
        event["tfTsvUpdatedAt"] = note["tfTsvUpdatedAt"]
        event["tfidfPngUpdatedAt"] = note["tfidfPngUpdatedAt"]
        event["isArchived"] = note["isArchived"]
    elif action == "update_tf":
        id_ = event["id"]
        content_updated_at = event["contentUpdatedAt"]
        is_archived = event["isArchived"]
        if is_archived:
            delete_tf_s3(id_)
        else:
            update_tf_s3(id_, content_updated_at)
    elif action == "update_idf":
        update_idf()
    elif action == "update_tfidf_png":
        id_ = event["id"]
        content_updated_at = event["contentUpdatedAt"]
        is_archived = event["isArchived"]
        if not is_archived:
            update_tf_idf_s3(id_, content_updated_at)
    elif action == "unfurl":
        event["attachement"] = unfurl_from_id(event["id"])
    else:
        logger.info(f"unknown action[{event['action']}]")
    return event
