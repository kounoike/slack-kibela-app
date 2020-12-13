import json
import logging
import os
import time
import datetime

from slack_bolt import App
from slack_bolt.adapter.aws_lambda import SlackRequestHandler
import aws_lambda_wsgi
from bottle import Bottle, request, response

import boto3

# process_before_response must be True when running on FaaS
ssm_client = boto3.client("ssm")
sfn_client = boto3.client("stepfunctions")

signing_secret_name = os.environ["SSM_SLACK_SIGNING_SECRET"]
token_name = os.environ["SSM_SLACK_BOT_TOKEN"]
ssm_response = ssm_client.get_parameters(Names=[signing_secret_name, token_name])
for param in ssm_response["Parameters"]:
    if param["Name"] == signing_secret_name:
        signing_secret = param["Value"]
    elif param["Name"] == token_name:
        token = param["Value"]
if not token or not signing_secret:
    raise Exception("can't retrieve ssm")


slack_app = App(
    signing_secret=signing_secret, token=token, process_before_response=True
)


# @app.middleware  # or app.use(log_request)
# def log_request(logger, body, next):
#     logger.debug(body)
#     return next()


command = "/hello-slack-kibela"


def respond_to_slack_within_3_seconds(body, ack, logger):
    if body.get("text") is None:
        ack(f":x: Usage: {command} (description here)")
    else:
        title = body["text"]
        ack(f"Accepted! (task: {title})")
        logger.info(f"***Accepted({title}).***")


def process_request(respond, body, logger):
    logger.info("***Start countdown.***")
    time.sleep(5)
    title = body["text"]
    logger.info(f"***Completed({title}).***")
    respond(f"Completed! (task: {title})")


slack_app.command(command)(
    ack=respond_to_slack_within_3_seconds, lazy=[process_request]
)


def fast_ack(ack, logger):
    logger.info("unfurl_kibela fast_ack")
    ack()


def unfurl_kibela(logger, event, client):
    logger.info(f"unfurl_kibela lazy {event} {client}")
    response = sfn_client.start_sync_execution(
        stateMachineArn=os.environ["UNFURL_STATEMACHINE_ARN"],
        input=json.dumps({"links": event["links"]}),
    )
    logger.info(f"Unfurl Response: {response}")
    output = json.loads(response["output"])
    logger.info(f"{output=}")
    unfurl_dict = dict(
        map(lambda x: (x["Payload"]["url"], x["Payload"]["attachement"]), output)
    )
    logger.info(f"sending unfurl. {unfurl_dict}")
    client.chat_unfurl(
        channel=event["channel"],
        ts=event["message_ts"],
        unfurls=json.dumps(unfurl_dict),
    )
    logger.info(f"unfurl done. {unfurl_dict}")


slack_app.event("link_shared")(ack=fast_ack, lazy=[unfurl_kibela])

SlackRequestHandler.clear_all_log_handlers()
# logging.basicConfig(format="%(asctime)s %(message)s", level=logging.DEBUG)
logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

slack_handler = SlackRequestHandler(app=slack_app)

bottle_app = Bottle()


@bottle_app.route("/")
def index():
    return {"OK": True}


@bottle_app.route("/kibela/webhook", method="POST")
def kibela_webhook():
    logging.info("kibela webhook received")
    req = request.json
    if req["resource_type"] in ["blog", "wiki"]:
        action = req["action"]
        url = req[req["resource_type"]]["url"]
        response = sfn_client.start_execution(
            stateMachineArn=os.environ["UPDATE_STATEMACHINE_ARN"],
            input=json.dumps({"url": url}),
        )
    return {"ok": True}


def handler(event, context):
    logging.info(f"Event: {event}")
    if "path" in event and event["path"].startswith("/slack/"):
        return slack_handler.handle(event, context)
    else:
        res = aws_lambda_wsgi.response(bottle_app, event, context)
        logging.info(f"{res=}")
        return res
